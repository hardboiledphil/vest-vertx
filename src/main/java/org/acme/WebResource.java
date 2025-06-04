package org.acme;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static org.acme.Channels.INTERNAL_EVENTS;

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
        VestEvent vestEvent = new VestEvent();
        vestEvent.setObjectId(objectid);
        vestEvent.setVersion(version);
        vestEvent.setInputXml("<test>Sample XML</test>");
        vestEvent.setCreated(new Date());
        vestEvent.setState(ProcessingState.FRESH);

        eventBus.send(INTERNAL_EVENTS, vestEvent);
        return Uni.createFrom().voidItem();
    }
}
