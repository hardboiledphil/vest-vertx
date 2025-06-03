package org.acme;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@ToString
@Getter
@Setter
public class VestEvent {

    private String          eventId;
    private MessageGroup    messageGroup;
    private String          objectId;
    private long            version;
    private ProcessingState state;
    private String          inputXml;
    private String          transformedXml;
    private Date            created;
    private Date            lastUpdated;

}
