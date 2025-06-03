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
        log.info("VestEvent findAndMerge called for objectId: {} version: {}",
                vestEvent.getObjectId(), vestEvent.getVersion());
        log.info("VestEvent findAndMerge state: {}", vestEvent.getState());

        return findById(vestEvent.getId())
                .onItem().ifNotNull().transform(existing -> {
                    log.info("Found existing VestEvent with id: {}", existing.getId());
                    // update only the fields that would have changed since creation
                    existing.updateProperties(vestEvent.getTransformedXml(), vestEvent.getState(), new Date());
                    // No need to persist - Panache will automatically flush changes
                    return existing;
                })
                .onItem().ifNull().fail()
                .chain(this::persist)
                .invoke(persistedEvent -> log.info("VestEvent with id: {} persisted", persistedEvent.getId()));
    }
}
