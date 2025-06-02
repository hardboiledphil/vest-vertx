package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@ApplicationScoped
public class Publisher {

    private final static Logger logger = LoggerFactory.getLogger(Publisher.class);

    @ConsumeEvent("publisher")
    public Uni<Void> send(final VestEvent vestEvent) {

        // Send the transformed XML to the appropriate queue
        val targetQueue = vestEvent.getTargetQueueName();
        val transformedXml = vestEvent.getTransformedXml();

        logger.info("Pretending to send transformed XML to queue: " + targetQueue);
        return Uni.createFrom().voidItem();
    }

    public void onStart(@Observes StartupEvent event) {
        System.out.println("Publisher is starting up at " + new Date());
        logger.info("Application starting up, initializing Publisher...");
    }
}
