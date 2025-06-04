package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.acme.Channels.COMPLETED_EVENTS;
import static org.acme.Channels.INTERNAL_EVENTS;
import static org.acme.Channels.PUBLISH_EVENTS;
import static org.acme.Channels.TRANSFORM_EVENTS;
import static org.acme.ProcessingState.PUBLISHED;
import static org.acme.ProcessingState.RECEIVED;

@Singleton
public class Processor {

    private final static Logger logger = LoggerFactory.getLogger(Processor.class);

    @Inject
    EventBus eventBus;

    final Map<String, VestEventHistory> vestEventHistoryMap = new ConcurrentHashMap<>();

    @ConsumeEvent(INTERNAL_EVENTS)
    protected void handleIncomingEvent(VestEvent event) {
        logger.info("Received event at Processor: {} version: {} state: {}",
                event.getObjectId(), event.getVersion(), event.getState());
        try {
            switch (event.getState()) {
                case RECEIVED -> initProcessEvent(event);
                case TRANSFORMED -> sendToProducer(event);
                case PUBLISHED -> postPublish(event);
                case ACK_RECEIVED -> logger.info("Ack received for {} version {} state: {}",
                        event.getObjectId(), event.getVersion(), event.getState());
                case APP_PROCESSED -> event.setState(ProcessingState.APP_PROCESSED);
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
        logger.info("Processing new event: {} version: {}",
                vestEvent.getObjectId(), vestEvent.getVersion());

        String key = vestEvent.getObjectId();

        VestEventHistory vestEventHistory;

        vestEvent.setState(RECEIVED);
        // No history for this objectid so create a new entry in the history map for it
        if (!vestEventHistoryMap.containsKey(key)) {
            logger.info("No history found for objectId {}. Creating new VestEventHistory.", key);
            // intentionally creating the hashmap with minimal size.
            var vestEventsMap = HashMap.<Long, VestEvent>newHashMap(1);
            vestEventsMap.put(vestEvent.getVersion(), vestEvent);
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
            if (vestEventHistory.getVestEventsMap().containsKey(vestEvent.getVersion())) {
                logger.warn("Event with version {} already exists for objectId {}. Ignoring this event.",
                        vestEvent.getVersion(), key);
                return; // Ignore duplicate events
            } else {
                // Add the new event to the existing history
                vestEventHistory.getVestEventsMap().put(vestEvent.getVersion(), vestEvent);
            }


        }
        // Forward to transformer
        eventBus.request(TRANSFORM_EVENTS, vestEvent)
                .subscribe().with(response -> {
                            // handle the response
                            VestEvent event1 = (VestEvent) response.body();
                            logger.info("Message received from transform process - send back to processor: {} version: {}",
                                    event1.getObjectId(), event1.getVersion());
                            // put the event back on the bus for further processing
                            eventBus.send(INTERNAL_EVENTS, event1);
                        },
                        failure -> {
                            // handle the failure
                            logger.error("Failed to process event in transformer: {}",
                                    vestEvent.getObjectId(), failure);
                        });
        // Store the event
        logger.info("Processor map now contains: {}", vestEventHistoryMap.toString());
        logger.info("Event processed by processor: {} version: {}", vestEvent.getObjectId(), vestEvent.getVersion());
    }

    protected void sendToProducer(VestEvent vestEvent) {

        //        // Check if we can process this version

        var lastProcessedVersion = vestEventHistoryMap.get(vestEvent.getObjectId()).getLastProcessedVersion();

        // if this version is the next one in sequence, we can process it
        if (vestEvent.getVersion() == lastProcessedVersion + 1) {
            eventBus.request(PUBLISH_EVENTS, vestEvent)
                    .subscribe().with(response -> {
                                // handle the response
                                VestEvent publishedEvent = (VestEvent) response.body();
                                logger.info("Message received from producer process - send back to processor: {} version: {}",
                                        publishedEvent.getObjectId(), publishedEvent.getVersion());
                                // get the source vest vestEvent from the history map
//                                var publishedEvent = vestEventHistoryMap.get(publishedEvent.getObjectId()).getVestEventsMap().
//                                        get(publishedEvent.getVersion());
                                vestEvent.setState(PUBLISHED);
                                eventBus.send(INTERNAL_EVENTS, vestEvent);
                            },
                            failure -> {
                                // handle the failure
                                logger.error("Failed to process vestEvent in producer: {}", vestEvent.getObjectId(), failure);
                            });

//            if (previousEvent == null || previousEvent.getState() != ProcessingState.APP_PROCESSED) {
//                logger.info("Cannot publish object {} version {} as previous version is not processed",
//                        vestEvent.getObjectId(), vestEvent.getVersion());
//                return;
//            }
        } else {
            logger.warn("Cannot send vestEvent for object {} version {}. Last processed version is {}",
                    vestEvent.getObjectId(), vestEvent.getVersion(), lastProcessedVersion);
            eventBus.send(COMPLETED_EVENTS, vestEvent);
        }

    }

    void postPublish(VestEvent vestEvent) {
        logger.info("Post-publish processing for event: {} version: {}", vestEvent.getObjectId(), vestEvent.getVersion());
        // if we have previously processed versions for this objectId in the event history map then
        // we can remove them
        String key = vestEvent.getObjectId();
        VestEventHistory vestEventHistory = vestEventHistoryMap.get(key);
        if (vestEventHistory != null) {
            // Update the last processed version
            vestEventHistory.setLastProcessedVersion(vestEvent.getVersion());
            // Remove all previous versions from the history map where they have been published
            // Get list of all previous versions
            vestEventHistory.getVestEventsMap().entrySet().removeIf(version -> {
                if (version.getValue().getVersion() < vestEvent.getVersion() && vestEvent.getState() == PUBLISHED) {
                    logger.info("Removing version {} for objectId {} from history", version, key);
                }
                return version.getValue().getVersion() < vestEvent.getVersion() && vestEvent.getState() == PUBLISHED;
            });
            logger.info("Updated history for objectId {}. Last processed version is now {}",
                    key, vestEventHistory.getLastProcessedVersion());
            logger.info("Processor map now contains: {}", vestEventHistoryMap.toString());
        } else {
            logger.warn("No history found for objectId {}. Cannot update last processed version.", key);
            return;
        }

        // send the event back to the coordinator so it can trigger the next event
        eventBus.send(COMPLETED_EVENTS, vestEvent);
    }

    void onStart(@Observes StartupEvent event) {
        logger.info("Application starting up, initializing Processor...");
    }

//    public void triggerSomething() {
//        System.out.println("Processor hashmap has a count of  " + this.vestEventMap.size());
//        System.out.println("Processor hashmap {}" + this.vestEventMap.toString());
//    }
}
