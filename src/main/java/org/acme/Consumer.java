package org.acme;

import java.util.Date;

import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.acme.Channels.INCOMING_EVENTS;

@ApplicationScoped
public class Consumer {

    private final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Inject
    EventBus eventBus;

    public void sendToSequencer() {
        logger.info("Sending to sequencer");
        
        // create a vest event
        VestEvent vestEvent = new VestEvent();
        vestEvent.setObjectId("ABC123");
        vestEvent.setVersion(1);
        vestEvent.setInputXml("<test>Sample XML</test>");
        vestEvent.setCreated(new Date());
        vestEvent.setState(ProcessingState.FRESH);

        // send the vest event to the sequencer
        System.out.println("sending message to the sequencer");
        eventBus.send(INCOMING_EVENTS, vestEvent);
        logger.info("Sent event to sequencer: {}", vestEvent.getObjectId());
    }

    public void onStart(@Observes StartupEvent event) {
        System.out.println("Consumer is starting up at " + new Date());
        logger.info("Application starting up, initializing consumer...");
//        sendToSequencer();
    }
}
