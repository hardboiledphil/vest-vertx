package org.acme;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.With;
import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

@Entity
@ToString
@Builder
@Getter
@With
@NoArgsConstructor
@AllArgsConstructor
public class VestEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue
    private Long id;
    @Enumerated(EnumType.STRING)
    private MessageGroup    messageGroup;
    private String          objectId;
    private long            version;
    @Enumerated(EnumType.STRING)
    private ProcessingState state;
    private String          inputXml;
    private String          transformedXml;
    private Date            created;
    private Date            lastUpdated;

    public void updateProperties(final String transformedXml, final ProcessingState state, final Date lastUpdated) {
        this.transformedXml = transformedXml;
        this.state = state;
        this.lastUpdated = lastUpdated;
    }
}
