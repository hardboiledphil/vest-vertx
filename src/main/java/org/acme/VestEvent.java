package org.acme;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@ToString
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
