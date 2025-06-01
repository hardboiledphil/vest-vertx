package org.acme;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@QuarkusMain
public class Main implements QuarkusApplication {

    private final static Logger logger = Logger.getLogger(Main.class);

    @Inject
    Consumer consumer;

    @Inject
    Sequencer sequencer;

    @Inject
    Transformer transformer;

    @Inject
    Publisher publisher;

    public static void main(String... args) {
        Quarkus.run(Main.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        logger.info("Starting application...");
        // The Consumer will be instantiated here due to the @Inject
        logger.info("Consumer initialized " +  consumer != null ? "successfully" : "failed");
        logger.info("Sequencer initialized " + sequencer != null ? "successfully" : "failed");
        logger.info("Transformer initialized " + transformer != null ? "successfully" : "failed");
        logger.info("Publisher initialized " + publisher != null ? "successfully" : "failed");
        // trigger the sequencer to start processing
        sequencer.triggerSomething();
        Quarkus.waitForExit();
        return 0;
    }
}


