package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static java.lang.Thread.sleep;
import static org.acme.Channels.PUBLISH_EVENTS;
import static org.acme.ProcessingState.PUBLISHED;

@ApplicationScoped
public class Publisher {

    private final static Logger logger = LoggerFactory.getLogger(Publisher.class);

    @Blocking
    @ConsumeEvent(PUBLISH_EVENTS)
    public Uni<VestEvent> send(final VestEvent vestEvent) throws InterruptedException {

        // Send the transformed XML to the appropriate queue
        val targetQueue = switch (vestEvent.getMessageGroup()) {
            case GOPS_PARCEL_SUB -> "gopsParcelSubQueue";
            case GOPS_EOD_CONTROL_SUB-> "gopsEodControlSubQueue";
            default -> "defaultQueue";
        };
        val transformedXml = vestEvent.getTransformedXml();
        vestEvent.setTransformedXml(vestEvent.getTransformedXml());
        vestEvent.setState(PUBLISHED);
        logger.info("Pretending to send transformed XML to queue: {}", targetQueue);
        sleep(100);
        return Uni.createFrom().item(vestEvent);
    }

    public void onStart(@Observes StartupEvent event) {
        System.out.println("Publisher is starting up at " + new Date());
        logger.info("Application starting up, initializing Publisher...");
    }
}
