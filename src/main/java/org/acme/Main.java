package org.acme;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusMain
public class Main implements QuarkusApplication {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    @Inject
    Consumer consumer;

    @Inject
    Processor processor;

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
        logger.info("Consumer initialized {}", consumer != null ? "successfully" : "failed");
        logger.info("Processor initialized {}", processor != null ? "successfully" : "failed");
        logger.info("Transformer initialized {}", transformer != null ? "successfully" : "failed");
        logger.info("Publisher initialized {}", publisher != null ? "successfully" : "failed");
        // trigger the processor to start processing
//        processor.triggerSomething();
        Quarkus.waitForExit();
        return 0;
    }
}


