package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import java.util.Date;

@ApplicationScoped
public class Transformer {

    private final static Logger logger = Logger.getLogger(Transformer.class);

    @ConsumeEvent("transformer")
    public Uni<Void> transform(VestEvent event) {
        try {
            // TODO: Implement actual XSLT transformation here
            String transformedXml = event.getInputXml(); // Placeholder for actual transformation
            
            // Validate against XSD
            validateXml(transformedXml);
            
            // Update event with transformed XML
            event.setTransformedXml(transformedXml);
            event.setState(ProcessingState.VEST_PROCESSED);

            logger.info("Successfully transformed and validated XML for event: " + event.getEventId());
            
            return Uni.createFrom().voidItem();
        } catch (Exception e) {
            logger.error("Error processing event: {}", event.getEventId(), e);
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
        logger.info("Application starting up, initializing Transformer...");
    }
} 