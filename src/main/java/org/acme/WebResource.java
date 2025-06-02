package org.acme;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;

import java.util.Date;

@Path("/event/send")
public class WebResource {

    @Inject
    EventBus eventBus;

    @GET
//    @Path("{objectid}&{version}")
//    public Uni<Void> send(final String objectid, final Integer version) {
    public Uni<Void> send() {

        // create a vest event
        VestEvent vestEvent = new VestEvent();
//        vestEvent.setObjectId(objectid);
        vestEvent.setObjectId("ABC124");
//        vestEvent.setVersion(version);
        vestEvent.setVersion(1);
        vestEvent.setInputXml("<test>Sample XML</test>");
        vestEvent.setCreated(new Date());
        vestEvent.setState(ProcessingState.FRESH);

        eventBus.send("incoming", vestEvent);
        return Uni.createFrom().voidItem();
    }
}
