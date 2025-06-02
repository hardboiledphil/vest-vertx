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

import static org.acme.Channels.INCOMING_EVENTS;
import static org.acme.Channels.PUBLISH_EVENTS;
import static org.acme.Channels.TRANSFORM_EVENTS;
import static org.acme.ProcessingState.PUBLISHED;

@Singleton
public class Sequencer {

    private final static Logger logger = LoggerFactory.getLogger(Sequencer.class);

    @Inject
    EventBus eventBus;

    final Map<String, VestEvent> vestEventMap = new ConcurrentHashMap<>();

    @ConsumeEvent(INCOMING_EVENTS)
    protected void handleIncomingEvent(VestEvent event) {
        logger.info("Received event at Sequencer: {} version: {} state: {}",
                event.getObjectId(), event.getVersion(), event.getState());
        try {
            switch (event.getState()) {
                case FRESH -> initProcessEvent(event);
                case TRANSFORMED -> sendToProducer(event);
                case PUBLISHED -> logger.info("Event publish completed for {} version {}",
                        event.getObjectId(), event.getVersion());
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

    protected void initProcessEvent(VestEvent event) {
        String key = event.getObjectId() + "_" + event.getVersion();

        if (vestEventMap.containsKey(key)) {
            logger.error("Duplicate event detected for {} - skipping event processing", key);
            return;
        }
        event.setState(ProcessingState.SEQUENCED);

        // Store the event
        vestEventMap.put(key, event);
        logger.info("map now contains: {}", vestEventMap.toString());

        // Forward to transformer
        eventBus.request(TRANSFORM_EVENTS, event)
                .subscribe().with(response -> {
                            // handle the response
                            VestEvent event1 = (VestEvent) response.body();
                            logger.info("Message received from transform process - send back to sequencer: {}", event1.getObjectId());
                            eventBus.send(INCOMING_EVENTS, event1);
                        },
                        failure -> {
                            // handle the failure
                            logger.error("Failed to process event in transformer: {}", event.getObjectId(), failure);
                        });

        logger.info("Event processed by sequencer: {}", key);
    }

    protected void sendToProducer(VestEvent event) {

        //        // Check if we can process this version
        if (event.getVersion() > 1) {
            String previousKey = event.getObjectId() + "_" + (event.getVersion() - 1);
            VestEvent previousEvent = vestEventMap.get(previousKey);

            if (previousEvent == null || previousEvent.getState() != ProcessingState.APP_PROCESSED) {
                logger.info("Cannot publish object {} version {} as previous version is not processed",
                        event.getObjectId(), event.getVersion());
                return;
            }
        }

        eventBus.request(PUBLISH_EVENTS, event)
                .subscribe().with(response -> {
                            // handle the response
                            VestEvent event1 = (VestEvent) response.body();
                            logger.info("Message received from producer process - send back to sequencer: {}", event1.getObjectId());
                            event1.setState(PUBLISHED);
                            eventBus.send(INCOMING_EVENTS, event1);
                        },
                        failure -> {
                            // handle the failure
                            logger.error("Failed to process event in producer: {}", event.getObjectId(), failure);
                        });
    }

    void onStart(@Observes StartupEvent event) {
        logger.info("Application starting up, initializing Sequencer...");
    }

//    public void triggerSomething() {
//        System.out.println("Sequencer hashmap has a count of  " + this.vestEventMap.size());
//        System.out.println("Sequencer hashmap {}" + this.vestEventMap.toString());
//    }
}
