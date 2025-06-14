package org.acme;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.acme.Channels.COMPLETED_EVENTS;
import static org.acme.Channels.INCOMING_EVENTS;
import static org.acme.Channels.INTERNAL_EVENTS;
import static org.acme.ProcessingState.RECEIVED;

@ApplicationScoped
public class Consumer {

    private final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Inject
    EventBus eventBus;

    @Inject
    VestEventRepository vestEventRepository;

    private static final Map<Tuple2<String, Long>, VestEvent> vestEventsToProcess = new ConcurrentHashMap<>();

    @ConsumeEvent(INCOMING_EVENTS)
    public void persistAndUpdateMap(final VestEvent vestEvent) {

        vestEvent.setState(RECEIVED);
        vestEvent.setLastUpdated(new Date());
        vestEventRepository.persist(vestEvent)
                .invoke(persistedEvent -> {
                    logger.info("Persisting VestEvent with id: {} for objectId: {}",
                            persistedEvent.getId(), persistedEvent.getObjectId());
                    // update the map with the persisted event
                    vestEventsToProcess.put(
                            Tuple2.of(persistedEvent.getObjectId(), persistedEvent.getVersion()), persistedEvent
                    );
                    logger.info("Consumer map now contains: {}", vestEventsToProcess);
                })
                // check if there's anything we can send to the processor
                .invoke(this::maybeSendToProcessor)
                .subscribe()
                .with(
                        persistedEvent -> logger.info("Persisted VestEvent with id: {} for objectId: {}",
                                persistedEvent.getId(), persistedEvent.getObjectId()),
                        failure -> logger.error("Failed to persist VestEvent: {}", failure.getMessage())
                );
    }

    Comparator<Map.Entry<Tuple2<String, Long>, VestEvent>> vestEventComparator =
            Comparator
            // first sort by the version
            .comparing((Map.Entry<Tuple2<String, Long>, VestEvent> entry)
                        -> entry.getKey().getItem2())
                    // then by the state, RECEIVED first
                    .thenComparing(entry ->
                            entry.getValue().getState() == ProcessingState.RECEIVED ? 0 : 1);

    protected void maybeSendToProcessor(VestEvent persistedEvent) {

        var eventsForObject = vestEventsToProcess.entrySet().stream()
                .filter(entry -> entry.getKey().getItem1().equals(persistedEvent.getObjectId()))
                .sorted(vestEventComparator)
                .toList();

        // a 1 RECEIVED
        // a 2 -----
        // a 3 TRANSFORMED
        // a 4 RECEIVED

        // sort by version and pick the first RECEIVED event if there is one - in this example we will send in v1
        // and then when it returns as transformed we will send v4
        for (Map.Entry<Tuple2<String, Long>, VestEvent> vestEvent : eventsForObject) {
            logger.info("Step 1) VestEvent in map: objectId: {} version: {} state: {}",
                    vestEvent.getKey().getItem1(), vestEvent.getKey().getItem2(), vestEvent.getValue().getState());
            var event = vestEvent.getValue();
            // should pick the earliest RECEIVED event and send it to the processor
            if (event.getState() == ProcessingState.RECEIVED) {
                event.setState(RECEIVED);
                logger.info("Sending RECEIVED event for objectId: {} version: {} to processor",
                        event.getObjectId(), event.getVersion());
                eventBus.send(INTERNAL_EVENTS, event);
                return;
            }
        }

        // a 1 PUBLISHED
        // a 2 TRANSFORMED
        // a 3 TRANSFORMED

        // ok so if we got here then there are no more RECEIVED events for this objectId
        // so we need to find the first event that is PUBLISHED (or is v1) and store the version.
        // If the next event version is TRANSFORMED then we can send it to the processor
        var lastProcessed = 0L;
        for (Map.Entry<Tuple2<String, Long>, VestEvent> vestEventEntry : eventsForObject) {
            logger.info("Step 2) VestEvent in map: objectId: {} version: {} state: {}",
                    vestEventEntry.getKey().getItem1(), vestEventEntry.getKey().getItem2(),
                    vestEventEntry.getValue().getState());
            var event = vestEventEntry.getValue();
            if (event.getState() == ProcessingState.PUBLISHED) {
                // if we have a PUBLISHED event then we can send the next TRANSFORMED event
                logger.info("Found PUBLISHED event for objectId: {} version: {}",
                        event.getObjectId(), event.getVersion());
                lastProcessed = event.getVersion();
            }
            if (event.getState() == ProcessingState.TRANSFORMED
                    && event.getVersion() == (lastProcessed + 1L)) {
                // if we have a TRANSFORMED event that's one version higher than the last processed
                // then we can send it to the processor
                logger.info("Sending TRANSFORMED event for objectId: {} version: {} to processor",
                        event.getObjectId(), event.getVersion());
                eventBus.send(INTERNAL_EVENTS, event);
            }
        }

//
//
//        eventsForObject.forEach(entry -> {
//            logger.info("Event in map: objectId: {} version: {} state: {}",
//                    entry.getKey().getItem1(), entry.getKey().getItem2(), entry.getValue().getState());
//        });
//
//        if (eventsForObject.size() == 1 &&
//                eventsForObject.getFirst().getValue().getState() == ProcessingState.RECEIVED) {
//
//            var eventToProcess = eventsForObject.getFirst().getValue();
//            eventToProcess.setState(RECEIVED);
//            logger.info("Sending single event for objectId: {} version: {} to processor",
//                    eventToProcess.getObjectId(), eventToProcess.getVersion());
//            eventBus.send(INTERNAL_EVENTS, eventToProcess);
//
//        } else if (eventsForObject.stream().allMatch(entry ->
//                entry.getValue().getState() == ProcessingState.RECEIVED)) {
//
//            var eventToProcess = eventsForObject.getFirst().getValue();
//            eventToProcess.setState(RECEIVED);
//            logger.info("Sending event for objectId: {} version: {} to processor",
//                    eventToProcess.getObjectId(), eventToProcess.getVersion());
//            eventBus.send(INTERNAL_EVENTS, eventToProcess);
//        }
    }

    @ConsumeEvent(COMPLETED_EVENTS)
    public void updateMap(final VestEvent vestEvent) {

        // update the vestEvent in the database
        vestEventRepository.findAndMerge(vestEvent)
                .invoke(updatedEvent -> {
                    if (updatedEvent.getState() == ProcessingState.PUBLISHED) {
                        // remove the event from the map if it was published
                        vestEventsToProcess.remove(Tuple2.of(updatedEvent.getObjectId(), updatedEvent.getVersion()));
                        logger.info("Removed from map VestEvent with id: {} for objectId: {} version: {}",
                                updatedEvent.getId(), updatedEvent.getObjectId(), updatedEvent.getVersion());
                    }
                }).subscribe().with(mergedEvent -> {
                            logger.info("Updated VestEvent with id: {} for objectId: {} version: {}",
                                    mergedEvent.getId(), mergedEvent.getObjectId(), mergedEvent.getVersion());
                    logger.info("Consumer map now contains: {}", vestEventsToProcess.toString());

                    // look for the next event to process
                    var eventsForThisObjectID = vestEventsToProcess.entrySet().stream()
                            .filter(entry -> entry.getKey().getItem1().equals(vestEvent.getObjectId()))
                            .toList();

                    // a 1 RECEIVED
                    // a 2 RECEIVED
                    // a 3 TRANSFORMED


                    // if we have only one event for this objectId then we can send it to the processor
                    if (eventsForThisObjectID.size() == 1
                            && eventsForThisObjectID.getFirst().getValue().getState() == ProcessingState.RECEIVED) {
                        var eventToProcess = eventsForThisObjectID.getFirst().getValue();
                        logger.info("Sending single event for objectId: {} version: {} to processor",
                                eventToProcess.getObjectId(), eventToProcess.getVersion());
                        eventBus.send(INTERNAL_EVENTS, eventToProcess);
                    } else if (!eventsForThisObjectID.stream().filter(entry ->
                            entry.getValue().getState() != ProcessingState.RECEIVED).toList().isEmpty()) {
                        // if we have more than one event and all of them are in the RECEIVED state
                        // then we send one to the processor
                        var eventToProcess = eventsForThisObjectID.getFirst().getValue();
                        logger.info("Sending event for objectId: {} version: {} to processor",
                                eventToProcess.getObjectId(), eventToProcess.getVersion());
                        eventBus.send(INTERNAL_EVENTS, eventToProcess);
                    }
                    // else we don't do anything, we wait for the next event
                        }, failure -> logger.error("Failed to update VestEvent: {}", failure.getMessage()));


    }


    public void onStart(@Observes StartupEvent event) {
//        sendToSequencer();
    }
}
