package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.acme.Channels.INCOMING_EVENTS;
import static org.acme.MessageGroup.GOPS_PARCEL_SUB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorTest.class);

    @Inject
    Processor processor;

    @Inject
    EventBus eventBus;

    @Inject
    Vertx vertx;

    @BeforeEach
    void setup() {
        // Clear the map before each test
        processor.vestEventHistoryMap.clear();
    }

    @Test
    void testHandleIncomingEvent() throws InterruptedException {
        // Create a test event
        VestEvent testEvent = VestEvent.builder().objectId("test123").version(1).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        CountDownLatch latch = new CountDownLatch(1);

        // Send the event through the event bus
        eventBus.send(INCOMING_EVENTS, testEvent);

        // Give some time for the event to be processed
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTrue(processor.vestEventHistoryMap.containsKey("test123"));
                    var eventHistory = processor.vestEventHistoryMap.get("test123").getVestEventsMap();
                    assertEquals(ProcessingState.PUBLISHED, eventHistory.get(1L).getState());
                    latch.countDown();
                });

        assertTrue(latch.await(6, TimeUnit.SECONDS));
    }

    @Test
    void testProcessingSequence() throws InterruptedException {
        // Create version 1 event
        VestEvent event1 = VestEvent.builder().objectId("testObj1").version(1).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        // Create version 2 event
        VestEvent event2 = VestEvent.builder().objectId("testObj1").version(2).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        CountDownLatch latch = new CountDownLatch(1);

        // Send first event
        eventBus.send(INCOMING_EVENTS, event1);

        // Wait and then check if processed
        vertx.setTimer(100, id -> {

            // Send second event
            eventBus.send(INCOMING_EVENTS, event2);

            // Give some time for the event to be processed
            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify both events are in the map
                assertTrue(processor.vestEventHistoryMap.containsKey("testObj1"));
                // Verify first event state was removed
                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj1")
                        .getVestEventsMap().get(1L).getState());
                assertEquals(ProcessingState.TRANSFORMED, processor.vestEventHistoryMap.get("testObj1")
                        .getVestEventsMap().get(2L).getState());
                latch.countDown();
            });
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test
    void testProcessingSequenceOutOfOrder() throws InterruptedException {
        // Create version 1 event
        VestEvent event1 = VestEvent.builder().objectId("testObj2").version(1).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        // Create version 2 event
        VestEvent event2 = VestEvent.builder().objectId("testObj2").version(2).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        // Create version 3 event
        VestEvent event3 = VestEvent.builder().objectId("testObj2").version(3).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        CountDownLatch latch = new CountDownLatch(1);

        eventBus.send(INCOMING_EVENTS, event3);
        eventBus.send(INCOMING_EVENTS, event2);
        eventBus.send(INCOMING_EVENTS, event1);

        // Wait and then check if processed
        vertx.setTimer(500, id -> {

            // Give some time for the event to be processed
            Awaitility.await().atMost(4, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify both events are in the map
                assertTrue(processor.vestEventHistoryMap.containsKey("testObj"));
                // Verify first event state was removed
//                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj")
//                        .getVestEventsMap().get(2L).getState());
//                logger.info("test3");
                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj")
                        .getVestEventsMap().get(3L).getState());
                latch.countDown();
            });
        });
        logger.info("test4");
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    void testDuplicateEventHandling() throws InterruptedException {
        // Create a test event
        VestEvent testEvent1 = VestEvent.builder().objectId("dupe").version(1).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        VestEvent testEvent2 = VestEvent.builder().objectId("dupe").version(1).messageGroup(GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH).inputXml("<xml>Content goes here </xml>").build();

        CountDownLatch latch = new CountDownLatch(1);

        // Send the same event twice
        eventBus.send(INCOMING_EVENTS, testEvent1);

        vertx.setTimer(100, id -> {
            // Send duplicate
            eventBus.send(INCOMING_EVENTS, testEvent2);

            Awaitility.await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify event only exists once in the map
                assertEquals(1, processor.vestEventHistoryMap.size());
                assertTrue(processor.vestEventHistoryMap.containsKey("dupe"));
                latch.countDown();
            });
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));

    }
}
