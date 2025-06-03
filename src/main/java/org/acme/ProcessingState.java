package org.acme;

public enum ProcessingState {

    FRESH,
    RECEIVED,
    SEQUENCED,
    TRANSFORMED,
    IN_PROCESSING,
    PUBLISHED,
    VEST_PROCESSED,
    ACK_RECEIVED,
    APP_PROCESSED,
}
