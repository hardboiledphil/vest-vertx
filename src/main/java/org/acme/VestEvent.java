package org.acme;

import io.quarkus.hibernate.reactive.panache.PanacheEntity;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;

import java.util.Date;

@Getter
@Entity
public class VestEvent extends PanacheEntity {

    @Id
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
