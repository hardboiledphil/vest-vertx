package org.acme;

import io.netty.channel.Channel;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.val;

@ApplicationScoped
public class Producer {

    @ConsumeEvent("producer")
    public Uni<Void> send(final VestEvent vestEvent) {

        // Send the transformed XML to the appropriate queue
        val targetQueue = vestEvent.getTargetQueueName();
        val transformedXml = vestEvent.getTransformedXml();

        return Uni.createFrom().voidItem();
    }
}
