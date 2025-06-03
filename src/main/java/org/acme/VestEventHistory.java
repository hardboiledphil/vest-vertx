package org.acme;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.HashMap;

@ToString
@Getter
@Setter
@Builder
public class VestEventHistory {

    private String                   objectId;
    private MessageGroup             messageGroup;
    private Long                     lastProcessedVersion;
    private HashMap<Long, VestEvent> vestEventsMap;

}
