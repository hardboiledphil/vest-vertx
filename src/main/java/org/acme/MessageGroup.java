package org.acme;

import lombok.Getter;

@Getter
public enum MessageGroup {

    GOPS_PARCEL_SUB,
    GOPS_PARCEL_PUB,
    EODOS_EOD_CONTROL_PUB,
    GOPS_EOD_CONTROL_SUB,
    GOPS_EOD_STATUS_PUB,
    EODOS_EOD_STATUS_SUB;

}
