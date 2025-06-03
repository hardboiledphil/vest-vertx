package org.acme;

import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@WithTransaction
public class VestEventHistoryRepository implements PanacheRepository<VestEventHistory> {

}
