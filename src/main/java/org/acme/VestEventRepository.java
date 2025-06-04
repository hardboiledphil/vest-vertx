package org.acme;

import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
@ApplicationScoped
@WithTransaction
public class VestEventRepository implements PanacheRepository<VestEvent> {

    public Uni<VestEvent> findAndMerge(VestEvent vestEvent) {
        log.info("VestEvent findAndMerge called for objectId: {} version: {} to state: {}",
                vestEvent.getObjectId(), vestEvent.getVersion(), vestEvent.getState());

        return findById(vestEvent.getId())
                .onItem().ifNotNull().transform(existing -> {
                    log.info("Found existing VestEvent with id: {} objectId: {} version: {} state: {}",
                            existing.getId(), existing.getObjectId(), existing.getVersion(), existing.getState());
                    // update only the fields that would have changed since creation
                    existing.setTransformedXml(vestEvent.getTransformedXml());
                    existing.setState(vestEvent.getState());
                    existing.setLastUpdated(new Date());
                    // No need to persist - Panache will automatically flush changes
                    return existing;
                })
                .onItem().ifNull().fail()
                .chain(this::persist)
                .invoke(persistedEvent ->
                        log.info("VestEvent with id: {} objectId: {} version: {} state: {} persisted",
                        persistedEvent.getId(), persistedEvent.getObjectId(),
                                persistedEvent.getVersion(), persistedEvent.getState()));
    }
}
