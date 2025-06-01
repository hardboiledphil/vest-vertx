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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class Sequencer {

    private final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Inject
    EventBus eventBus;

    private final Map<String, VestEvent> vestEventMap = new ConcurrentHashMap<>();

    @ConsumeEvent("incoming")
    protected void handleIncomingEvent(VestEvent event) {
        System.out.println("Received event at Sequencer: " + event.getObjectId() + " version: " + event.getVersion());
        try {
            processEvent(event);
        } catch (Exception e) {
            logger.error("Error processing incoming message", e);
        }
    }

    protected void processEvent(VestEvent event) {
        String key = event.getObjectId() + "_" + event.getVersion();

        if (vestEventMap.containsKey(key)) {
            logger.error("Duplicate event detected for {} - skipping event processing", key);
            return;
        }

        // Store the event
        vestEventMap.put(key, event);

        // Forward to transformer
        eventBus.send("transformer", event);

        // Check if we can process this version
        if (event.getVersion() > 1) {
            String previousKey = event.getObjectId() + "_" + (event.getVersion() - 1);
            VestEvent previousEvent = vestEventMap.get(previousKey);
            
            if (previousEvent == null || previousEvent.getState() != ProcessingState.VEST_PROCESSED) {
                logger.info("Cannot process version {} for object {} as previous version is not processed", event.getVersion(), event.getObjectId());
                return;
            }
        }

        eventBus.send("publisher", event);

        logger.info("Event processed by sequencer: {}", key);
    }

    protected void updateEventState(String objectId, long version, ProcessingState newState) {
        String key = objectId + "_" + version;
        VestEvent event = vestEventMap.get(key);
        if (event != null) {
            event.setState(newState);
            logger.info("Updated state for {} to {}", key, newState);
        }
    }

    void onStart(@Observes StartupEvent event) {
        System.out.println("Sequencer is starting up at " + new Date());
        logger.info("Application starting up, initializing Sequencer...");
    }

    public void triggerSomething() {
        System.out.println("Sequencer hashmap has a count of  " + this.vestEventMap.size());
        System.out.println("Sequencer hashmap {}" + this.vestEventMap.toString());
    }
}
