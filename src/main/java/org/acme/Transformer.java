package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import lombok.extern.slf4j.Slf4j;

import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import java.util.Date;

import static java.lang.Thread.sleep;

@ApplicationScoped
@Slf4j
public class Transformer {

    @ConsumeEvent("transformer")
    @Blocking
    public Uni<Void> transform(VestEvent event) {
        try {
            // TODO: Implement actual XSLT transformation here
            String transformedXml = event.getInputXml(); // Placeholder for actual transformation
            
            // Validate against XSD
            validateXml(transformedXml);

            sleep(2000);

            // Update event with transformed XML
            event.setTransformedXml(transformedXml);
            event.setState(ProcessingState.VEST_PROCESSED);

            log.info("Successfully transformed and validated XML for event: {}", event.getEventId());
            
            return Uni.createFrom().voidItem();
        } catch (Exception e) {
            log.error("Error processing event: {}", event.getEventId(), e);
            return Uni.createFrom().failure(e);
        }
    }
    
    private void validateXml(String xml) throws Exception {
        // TODO: Implement actual XSD validation
        // This is a placeholder for the actual validation logic
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        // Schema schema = factory.newSchema(new StreamSource(new StringReader(xsdContent)));
        // Validator validator = schema.newValidator();
        // validator.validate(new StreamSource(new StringReader(xml)));
    }

    void onStart(@Observes StartupEvent event) {
        System.out.println("Transformer is starting up at " + new Date());
        log.info("Application starting up, initializing Transformer...");
    }
} 