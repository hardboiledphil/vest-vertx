package org.acme;

import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
@Slf4j
@ApplicationScoped
@WithTransaction
public class VestEventHistoryRepository implements PanacheRepository<VestEventHistory> {

    public Uni<VestEventHistory> findAndMerge(VestEventHistory vestEventHistory) {
        log.info("VestEventHistory findAndMerge called for id: {} objectId: {}",
                vestEventHistory.getId(), vestEventHistory.getObjectId());

        return findById(vestEventHistory.getId())
                .onItem().ifNotNull().transform(existing -> {
                    log.info("Found existing VestEventHistory with id: {}", existing.getId());
                    // update only the fields that would have changed since creation
                    existing.setLastProcessedVersion(vestEventHistory.getLastProcessedVersion());
                    existing.setVestEventsMap(vestEventHistory.getVestEventsMap());
                    // No need to persist - Panache will automatically flush changes
                    return existing;
                })
                .onItem().ifNull().switchTo(() -> {
                    // If no entity exists, persist a new one
                    return persist(vestEventHistory);})
                .chain(this::persist)
                .invoke(persistedEvent -> log.info("VestEventHistory with id: {} persisted", persistedEvent.getId()));
    }
}
