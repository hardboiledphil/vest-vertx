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
        VestEvent testEvent = new VestEvent();
        testEvent.setObjectId("test123");
        testEvent.setVersion(1);
        testEvent.setState(ProcessingState.FRESH);
        testEvent.setMessageGroup(GOPS_PARCEL_SUB);
        testEvent.setInputXml("<xml>Content goes here </xml>");

        CountDownLatch latch = new CountDownLatch(1);

        // Send the event through the event bus
        eventBus.send(INCOMING_EVENTS, testEvent);

        // Give some time for the event to be processed
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTrue(processor.vestEventHistoryMap.containsKey("test123"));
                    var eventHistory = processor.vestEventHistoryMap.get("test123").getVestEventsMap();
                    assertEquals(ProcessingState.PUBLISHED, eventHistory.get(1L).getState());
                    latch.countDown();
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    void testProcessingSequence() throws InterruptedException {
        // Create version 1 event
        VestEvent event1 = new VestEvent();
        event1.setObjectId("testObj");
        event1.setVersion(1);
        event1.setMessageGroup(GOPS_PARCEL_SUB);
        event1.setState(ProcessingState.FRESH);

        // Create version 2 event
        VestEvent event2 = new VestEvent();
        event2.setObjectId("testObj");
        event2.setVersion(2);
        event2.setMessageGroup(GOPS_PARCEL_SUB);
        event2.setState(ProcessingState.FRESH);

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
                assertTrue(processor.vestEventHistoryMap.containsKey("testObj"));
                // Verify first event state was removed
                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj")
                        .getVestEventsMap().get(1L).getState());
                assertEquals(ProcessingState.TRANSFORMED, processor.vestEventHistoryMap.get("testObj")
                        .getVestEventsMap().get(2L).getState());
                latch.countDown();
            });
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }


    @Test
    void testProcessingSequenceOutOfOrder() throws InterruptedException {
        // Create version 1 event
        VestEvent event1 = new VestEvent();
        event1.setObjectId("testObj");
        event1.setVersion(1);
        event1.setMessageGroup(GOPS_PARCEL_SUB);
        event1.setState(ProcessingState.FRESH);

        // Create version 2 event
        VestEvent event2 = new VestEvent();
        event2.setObjectId("testObj");
        event2.setVersion(2);
        event2.setMessageGroup(GOPS_PARCEL_SUB);
        event2.setState(ProcessingState.FRESH);

        // Create version 3 event
        VestEvent event3 = new VestEvent();
        event3.setObjectId("testObj");
        event3.setVersion(3);
        event3.setMessageGroup(GOPS_PARCEL_SUB);
        event3.setState(ProcessingState.FRESH);

        CountDownLatch latch = new CountDownLatch(1);

        // Send first event
        eventBus.send(INCOMING_EVENTS, event3);

        // Wait and then check if processed
        vertx.setTimer(500, id -> {

            // Send second and first event
            eventBus.send(INCOMING_EVENTS, event2);
            eventBus.send(INCOMING_EVENTS, event1);

            // Give some time for the event to be processed
            Awaitility.await().atMost(4, TimeUnit.SECONDS).untilAsserted(() -> {
                // Verify both events are in the map
                assertTrue(processor.vestEventHistoryMap.containsKey("testObj"));
                // Verify first event state was removed
                logger.info("test1");
                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj")
                        .getVestEventsMap().get(1L).getState());
                logger.info("test2");
                assertEquals(ProcessingState.PUBLISHED, processor.vestEventHistoryMap.get("testObj")
                        .getVestEventsMap().get(2L).getState());
                logger.info("test3");
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
        VestEvent testEvent1 = new VestEvent();
        testEvent1.setObjectId("dupe");
        testEvent1.setVersion(1);
        testEvent1.setMessageGroup(GOPS_PARCEL_SUB);
        testEvent1.setState(ProcessingState.FRESH);

        VestEvent testEvent2 = new VestEvent();
        testEvent2.setObjectId("dupe");
        testEvent2.setVersion(1);
        testEvent2.setMessageGroup(GOPS_PARCEL_SUB);
        testEvent2.setState(ProcessingState.FRESH);

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
