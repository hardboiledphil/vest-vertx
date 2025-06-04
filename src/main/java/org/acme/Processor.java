package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.acme.Channels.INCOMING_EVENTS;
import static org.acme.Channels.PUBLISH_EVENTS;
import static org.acme.Channels.TRANSFORM_EVENTS;
import static org.acme.ProcessingState.PUBLISHED;
import static org.acme.ProcessingState.RECEIVED;

@Singleton
public class Processor {

    protected Processor() {
        // Default constructor for CDI
    }

    private final static Logger logger = LoggerFactory.getLogger(Processor.class);

    @Inject
    EventBus eventBus;

    @Inject
    VestEventHistoryRepository vestEventHistoryRepository;

    @Inject
    VestEventRepository vestEventRepository;

    final Map<String, VestEventHistory> vestEventHistoryMap = new ConcurrentHashMap<>();

    @ConsumeEvent(INCOMING_EVENTS)
    protected void handleIncomingEvent(VestEvent event) {
        logger.info("Received event at Processor: {} version: {} state: {}",
                event.getObjectId(), event.getVersion(), event.getState());
        try {
            switch (event.getState()) {
                case FRESH -> initProcessEvent(event);
                case RECEIVED -> sendToTransformer(event);
                case TRANSFORMED -> sendToProducer(event);
                case PUBLISHED -> postPublish(event);
                case ACK_RECEIVED -> logger.info("Ack received for {} version {} state: {}",
                        event.getObjectId(), event.getVersion(), event.getState());
                case APP_PROCESSED -> markAsProcessed(event);
                default -> {
                    logger.warn("Received event with unexpected state: {}", event.getState());
                    return; // Ignore events that are not in the expected state
                }
            }
        } catch (Exception e) {
            logger.error("Error processing incoming message", e);
        }
    }

    protected void initProcessEvent(VestEvent vestEvent) {
        logger.info("Processing id: {} event: {} version: {}",
                vestEvent.getId(), vestEvent.getObjectId(), vestEvent.getVersion());

        String key = vestEvent.getObjectId();
        Long version = vestEvent.getVersion();

        VestEventHistory vestEventHistory;

        vestEvent.setState(RECEIVED);
        vestEvent.setCreated(new Date());
        vestEvent.setLastUpdated(new Date());

        // No history for this objectid so create a new entry in the history map for it
        if (!vestEventHistoryMap.containsKey(key)) {
            logger.info("No history found for objectId {}. Creating new VestEventHistory", key);
            // intentionally creating the hashmap with minimal size.
            var vestEventsMap = HashMap.<Long, VestEvent>newHashMap(1);
            vestEventsMap.put(version, vestEvent);
            vestEventHistory = VestEventHistory.builder()
                    .objectId(key)
                    .messageGroup(MessageGroup.GOPS_PARCEL_SUB)
                    .lastProcessedVersion(0L)
                    .vestEventsMap(vestEventsMap)
                    .build();
            vestEventHistoryMap.put(key, vestEventHistory);
        } else {
            logger.info("Found existing history for objectId {}. Re-using it.", key);
            vestEventHistory = vestEventHistoryMap.get(key);
            if (vestEventHistory.getVestEventsMap().containsKey(version)) {
                logger.warn("Event with version {} already exists for objectId {}. Ignoring this event.",
                        version, key);
                return; // Ignore duplicate events
            } else {
                // Add the new event to the existing history
                vestEventHistory.getVestEventsMap().put(version, vestEvent);
            }
        }

        vestEventHistoryRepository.persist(vestEventHistory)
                .onFailure().invoke(failure ->
                        logger.error("Failed to save VestEventHistory for objectId {}: version: {}",
                                key, version, failure))
                .subscribe().with(savedHistory -> {
                    // Successfully saved the history, now we can proceed with processing
                    logger.info("SUBSCRIBE WITH called VestEventHistory for {} lastProcessed version: {}",
                            savedHistory.getObjectId(), savedHistory.getLastProcessedVersion());
                    logger.info("map now contains: {}", vestEventHistoryMap.toString());
                    eventBus.send(INCOMING_EVENTS, vestEvent);
                });
    }

    protected void sendToTransformer(VestEvent event) {
        logger.info("Sending event to transformer: {} version: {}", event.getObjectId(), event.getVersion());

        // make a request to transformer
        eventBus.request(TRANSFORM_EVENTS, event)
                .onItem().transform(result -> {
                    // handle the response
                    VestEvent transformedEvent = (VestEvent) result.body();
                    logger.info("Transformed id: {} event: {} version: {}",
                            transformedEvent.getId(), transformedEvent.getObjectId(), transformedEvent.getVersion());
                    // Update the state to TRANSFORMED
                    transformedEvent.setState(ProcessingState.TRANSFORMED);
                    transformedEvent.setLastUpdated(new Date());
                    return transformedEvent;
                })
                .onItem().transformToUni(updatedEvent -> vestEventRepository.findAndMerge(updatedEvent))
                .subscribe().with(savedEvent -> {
                    // handle the response
                    logger.info("VestEvent id {} objectId {} version {} saved after transformation",
                            savedEvent.getId(), savedEvent.getObjectId(), savedEvent.getVersion());

                    // put the event back on the bus for further processing
                    eventBus.send(INCOMING_EVENTS, savedEvent);
                    logger.info("map now contains: {}", vestEventHistoryMap.toString());
                    logger.info("Event processed by processor: id: {} {} version: {}",
                            savedEvent.getId(), savedEvent.getObjectId(), savedEvent.getVersion());
                });
    }

    protected void sendToProducer(VestEvent event) {

        // Check if we can process this version
        var lastProcessedVersion = vestEventHistoryMap.get(event.getObjectId()).getLastProcessedVersion();

        // if this version is the next one in sequence, we can process it
        if (event.getVersion() == lastProcessedVersion + 1) {
            // Send a request to the producer to publish the event
            eventBus.request(PUBLISH_EVENTS, event)
                    // on failure log an error
                    .onFailure().invoke(failure ->
                            logger.error("Failed to send event to producer for objectId {} version {}",
                                    event.getObjectId(), event.getVersion(), failure))
                    .onItem().transform(result -> {
                        // handle the response
                        VestEvent sentEvent = (VestEvent) result.body();
                        logger.info("Sent id: {} event: {} version: {}",
                                sentEvent.getId(), sentEvent.getObjectId(), sentEvent.getVersion());
                        // Update the state to PUBLISHED
                        sentEvent.setState(ProcessingState.PUBLISHED);
                        sentEvent.setLastUpdated(new Date());
                        return sentEvent;
                    })
                    .onItem().transformToUni(updatedEvent -> vestEventRepository.findAndMerge(updatedEvent))
                    // on success send the event back to the processor for its next step
                    .subscribe().with(savedEvent -> {
                                logger.info("Message received from producer process - send back to processor: id: {} objectId: {} version: {} status: {}",
                                        savedEvent.getId(), savedEvent.getObjectId(), savedEvent.getVersion(), savedEvent.getState());
                                logger.info("map now contains: {}", vestEventHistoryMap.toString());
                                VestEventHistory vestEventHistory = vestEventHistoryMap.get(savedEvent.getObjectId());
                                vestEventHistory.getVestEventsMap().put(savedEvent.getVersion(), savedEvent);
                                eventBus.send(INCOMING_EVENTS, savedEvent);
                            },
                            failure -> {
                                // handle the failure
                                logger.error("Failed to process event in producer: {}", event.getObjectId(), failure);
                            });
        } else {
            logger.warn("Cannot send event for object {} version {}. Last processed version is {}",
                    event.getObjectId(), event.getVersion(), lastProcessedVersion);
        }
    }

    protected void postPublish(VestEvent event) {
        logger.info("Post-publish processing for event: {} version: {}", event.getObjectId(), event.getVersion());
        // if we have previously processed versions for this objectId in the event history map then
        // we can remove them
        String key = event.getObjectId();
        VestEventHistory vestEventHistory = vestEventHistoryMap.get(key);
        if (vestEventHistory != null) {
            // Update the last processed version
            vestEventHistory.setLastProcessedVersion(event.getVersion());
            // Remove all previous versions from the history map where they have been published
            // Get list of all previous versions
            vestEventHistory.getVestEventsMap().entrySet().removeIf(version -> {
                if (version.getValue().getVersion() < event.getVersion() && event.getState() == PUBLISHED) {
                    logger.info("Removing version {} for objectId {} from history", version, key);
                }
                return version.getValue().getVersion() < event.getVersion() && event.getState() == PUBLISHED;
            });

            vestEventHistoryRepository.findAndMerge(vestEventHistory)
                    .subscribe().with(
                            success -> logger.info("Successfully updated VestEventHistory for objectId {}",
                                    key),
                            failure -> logger.error("Failed to update VestEventHistory for objectId {}",
                                    key, failure));
            logger.info("Updated history for objectId {}. Last processed version is now {}",
                    key, vestEventHistory.getLastProcessedVersion());
            logger.info("map now contains: {}", vestEventHistoryMap.toString());
        } else {
            logger.warn("No history found for objectId {}. Cannot update last processed version.", key);
            return;
        }

        // We should be able to take the next available event from the history map and if it's transformed
        // then we can publish it
        vestEventHistory.getVestEventsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .filter(entry ->
                        entry.getKey() == vestEventHistory.getLastProcessedVersion() + 1
                                && entry.getValue().getState() == ProcessingState.TRANSFORMED)
                .findFirst().ifPresent(nextEvent -> {
                    logger.info("Sending next event objectid: {} version: {} to producer for publishing",
                            nextEvent.getValue().getObjectId(), nextEvent.getValue().getVersion());
                    // Send the next event to the producer so it can trigger publishing
                    eventBus.send(INCOMING_EVENTS, nextEvent.getValue());
                });
    }

    protected void markAsProcessed(VestEvent vestEvent) {
        logger.info("Marking event as processed: {} version: {}", vestEvent.getObjectId(), vestEvent.getVersion());
        // Update the state of the event to APP_PROCESSED
        VestEventHistory vestEventHistory = vestEventHistoryMap.get(vestEvent.getObjectId());
        if (vestEventHistory != null) {
//            VestEvent vestEventLatest = vestEventHistory.getVestEventsMap().get(vestEvent.getVersion());
            vestEvent.setState(ProcessingState.APP_PROCESSED);
            vestEventRepository.findAndMerge(vestEvent)
                    .subscribe().with(
                            success -> logger.info("Successfully marked event as processed: {} version: {}",
                                    vestEvent.getObjectId(), vestEvent.getVersion()),
                            failure -> logger.error("Failed to mark event as processed: {} version: {}",
                                    vestEvent.getObjectId(), vestEvent.getVersion(), failure));
        } else {
            logger.warn("No history found for objectId {}. Cannot mark as processed.", vestEvent.getObjectId());
        }
    }

    protected void onStart(@Observes StartupEvent event) {
        logger.info("Application starting up, initializing Processor...");
    }
}
