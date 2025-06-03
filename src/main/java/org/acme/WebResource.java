package org.acme;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static org.acme.Channels.INCOMING_EVENTS;

@Path("/event/send")
public class WebResource {

    private final static Logger logger = LoggerFactory.getLogger(WebResource.class);

    @Inject
    EventBus eventBus;

    /**
     * To create a new event via http
     * @param objectid
     * @param version
     * @return
     */
    @GET
    @Path("{objectid}/{version}")
    public Uni<Void> send(final String objectid, final Integer version) {

        // create a vest event
        VestEvent vestEvent = VestEvent.builder()
                .objectId(objectid)
                .version(version)
                .messageGroup(MessageGroup.GOPS_PARCEL_SUB)
                .state(ProcessingState.FRESH)
                .inputXml("<test>Sample XML</test>")
                .created(new Date())
                .build();

        eventBus.send(INCOMING_EVENTS, vestEvent);
        return Uni.createFrom().voidItem();
    }
}
