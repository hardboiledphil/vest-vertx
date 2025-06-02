package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.acme.Channels.INCOMING_EVENTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class SequencerTest {

    @Inject
    Sequencer sequencer;

    @Inject
    EventBus eventBus;

    @Inject
    Vertx vertx;

    @BeforeEach
    void setup() {
        // Clear the map before each test
        sequencer.vestEventMap.clear();
    }

    @Test
    void testHandleIncomingEvent() throws InterruptedException {
        // Create a test event
        VestEvent testEvent = new VestEvent();
        testEvent.setObjectId("test123");
        testEvent.setVersion(1);
        testEvent.setState(ProcessingState.FRESH);

        CountDownLatch latch = new CountDownLatch(1);

        // Send the event through the event bus
        eventBus.send(INCOMING_EVENTS, testEvent);

        // Give some time for the event to be processed
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTrue(sequencer.vestEventMap.containsKey("test123_1"));
                    assertEquals(ProcessingState.PUBLISHED, sequencer.vestEventMap.get("test123_1").getState());
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
        event1.setState(ProcessingState.FRESH);

        // Create version 2 event
        VestEvent event2 = new VestEvent();
        event2.setObjectId("testObj");
        event2.setVersion(2);
        event2.setState(ProcessingState.FRESH);

        CountDownLatch latch = new CountDownLatch(1);

        // Send first event
        eventBus.send(INCOMING_EVENTS, event1);

        // Wait and then check if processed
        vertx.setTimer(100, id -> {

            // Send second event
            eventBus.send(INCOMING_EVENTS, event2);

            // Give some time for the event to be processed
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                // Verify both events are in the map
                assertTrue(sequencer.vestEventMap.containsKey("testObj_1"));
                assertTrue(sequencer.vestEventMap.containsKey("testObj_2"));

                // Verify first event state was updated
                assertEquals(ProcessingState.PUBLISHED, sequencer.vestEventMap.get("testObj_1").getState());
                assertEquals(ProcessingState.TRANSFORMED, sequencer.vestEventMap.get("testObj_2").getState());

                latch.countDown();
            });
        });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @Test
    void testDuplicateEventHandling() throws InterruptedException {
        // Create a test event
        VestEvent testEvent1 = new VestEvent();
        testEvent1.setObjectId("dupe");
        testEvent1.setVersion(1);
        testEvent1.setState(ProcessingState.FRESH);

        VestEvent testEvent2 = new VestEvent();
        testEvent2.setObjectId("dupe");
        testEvent2.setVersion(1);
        testEvent2.setState(ProcessingState.FRESH);

        CountDownLatch latch = new CountDownLatch(1);

        // Send the same event twice
        eventBus.send(INCOMING_EVENTS, testEvent1);

        vertx.setTimer(100, id -> {
            // Send duplicate
            eventBus.send(INCOMING_EVENTS, testEvent2);

            Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                // Verify event only exists once in the map
                assertEquals(1, sequencer.vestEventMap.size());
                assertTrue(sequencer.vestEventMap.containsKey("dupe_1"));
                        latch.countDown();
            });
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));

    }
}
