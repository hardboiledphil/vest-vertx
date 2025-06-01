package org.acme;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class VestEvent {

    private String       eventId;
    private String       objectId;
    private long         version;
    private MessageGroup messageGroup;
    private String       inputXml;
    private Date         created;
    private Date         lastUpdated;
    private ProcessingState state;
    private String       targetQueueName;
    private String       transformedXml;

}
