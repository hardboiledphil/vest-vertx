package org.acme;

import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ConsumerTest {
    private Consumer consumer;
    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        consumer = new Consumer();
        eventBus = mock(EventBus.class);
        // Use reflection to inject the mock EventBus
        try {
            var field = Consumer.class.getDeclaredField("eventBus");
            field.setAccessible(true);
            field.set(consumer, eventBus);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Clear the static map before each test
        Map<?, ?> map = getVestEventsToProcess();
        map.clear();
    }

    @SuppressWarnings("unchecked")
    private Map<Tuple2<String, Long>, VestEvent> getVestEventsToProcess() {
        try {
            var field = Consumer.class.getDeclaredField("vestEventsToProcess");
            field.setAccessible(true);
            return (Map<Tuple2<String, Long>, VestEvent>) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSingleFreshEventSendsToProcessor() {
        var event = VestEvent.builder()
                .objectId("obj1")
                .version(1L)
                .state(ProcessingState.RECEIVED)
                .build();
        getVestEventsToProcess().put(Tuple2.of("obj1", 1L), event);
        consumer.maybeSendToProcessor(event);
        verify(eventBus, times(1)).send(anyString(), eq(event));
        assertEquals(ProcessingState.RECEIVED, event.getState());
    }

    @Test
    void testAllFreshEventsSendsFirstToProcessor() {
        var event1 = VestEvent.builder()
                .objectId("obj2")
                .version(1L)
                .state(ProcessingState.RECEIVED)
                .build();
        var event2 = VestEvent.builder()
                .objectId("obj2")
                .version(2L)
                .state(ProcessingState.RECEIVED)
                .build();
        getVestEventsToProcess().put(Tuple2.of("obj2", 1L), event1);
        getVestEventsToProcess().put(Tuple2.of("obj2", 2L), event2);
        consumer.maybeSendToProcessor(event1);
        verify(eventBus, times(1)).send(anyString(), eq(event1));
        assertEquals(ProcessingState.RECEIVED, event1.getState());
    }

    @Test
    void testFistIsFreshEventsSendsFirstToProcessor() {
        var event1 = VestEvent.builder()
                .objectId("obj2")
                .version(1L)
                .state(ProcessingState.RECEIVED)
                .build();
        var event2 = VestEvent.builder()
                .objectId("obj2")
                .version(2L)
                .state(ProcessingState.TRANSFORMED)
                .build();
        getVestEventsToProcess().put(Tuple2.of("obj2", 1L), event1);
        getVestEventsToProcess().put(Tuple2.of("obj2", 2L), event2);
        consumer.maybeSendToProcessor(event1);
        verify(eventBus, times(1)).send(anyString(), eq(event1));
        assertEquals(ProcessingState.RECEIVED, event1.getState());
        assertEquals(ProcessingState.TRANSFORMED, event2.getState());
    }

    @Test
    void testNoFreshEventDoesNotSend() {
        var event = VestEvent.builder()
                .objectId("obj3")
                .version(2L)
                .state(ProcessingState.TRANSFORMED)
                .build();
        getVestEventsToProcess().put(Tuple2.of("obj3", 2L), event);
        consumer.maybeSendToProcessor(event);
        verify(eventBus, never()).send(anyString(), any());
        assertEquals(ProcessingState.TRANSFORMED, event.getState());
    }

    @Test
    void testVestEventComparatorSortsByVersion() {
        var event1 = VestEvent.builder().objectId("o").version(1L).state(ProcessingState.RECEIVED).build();
        var event2 = VestEvent.builder().objectId("o").version(2L).state(ProcessingState.RECEIVED).build();
        var entry1 = Map.entry(Tuple2.of("o", 1L), event1);
        var entry2 = Map.entry(Tuple2.of("o", 2L), event2);
        var list = new java.util.ArrayList<>(java.util.List.of(entry2, entry1));
        list.sort(consumer.vestEventComparator);
        assertEquals(1L, list.get(0).getKey().getItem2());
        assertEquals(2L, list.get(1).getKey().getItem2());
    }

    @Test
    void testVestEventComparatorPutsFreshFirstIfSameVersion() {
        var eventFresh = VestEvent.builder().objectId("o").version(1L).state(ProcessingState.RECEIVED).build();
        var eventOther = VestEvent.builder().objectId("o").version(1L).state(ProcessingState.TRANSFORMED).build();
        var entryFresh = Map.entry(Tuple2.of("o", 1L), eventFresh);
        var entryOther = Map.entry(Tuple2.of("o", 1L), eventOther);
        var list = new java.util.ArrayList<>(java.util.List.of(entryOther, entryFresh));
        list.sort(consumer.vestEventComparator);
        assertSame(eventFresh, list.get(0).getValue());
        assertSame(eventOther, list.get(1).getValue());
    }

    @Test
    void testVestEventComparatorSortsByVersionThenFresh() {
        var event1 = VestEvent.builder().objectId("o").version(1L).state(ProcessingState.TRANSFORMED).build();
        var event2 = VestEvent.builder().objectId("o").version(2L).state(ProcessingState.RECEIVED).build();
        var event3 = VestEvent.builder().objectId("o").version(3L).state(ProcessingState.RECEIVED).build();
        var entry1 = Map.entry(Tuple2.of("o", 1L), event1);
        var entry2 = Map.entry(Tuple2.of("o", 2L), event2);
        var entry3 = Map.entry(Tuple2.of("o", 3L), event3);
        var list = new java.util.ArrayList<>(java.util.List.of(entry3, entry2, entry1));
        list.sort(consumer.vestEventComparator);
        assertEquals(1L, list.get(0).getKey().getItem2());
        assertEquals(2L, list.get(1).getKey().getItem2());
        assertEquals(3L, list.get(2).getKey().getItem2());
        assertSame(event1, list.get(0).getValue());
        assertSame(event2, list.get(1).getValue());
        assertSame(event3, list.get(2).getValue());
    }
}
