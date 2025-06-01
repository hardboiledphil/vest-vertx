package org.acme;

import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class Sequencer {

    @Inject
    EventBus eventBus;

    private ConcurrentHashMap<String, VestEvent> vestEntryMap;

    // 1) receives an event message


    // 2) Get the OBJECTID & VERSION


    // 3) Create an event record and persist it to the db


    // 4) Add record to the processing table



}
