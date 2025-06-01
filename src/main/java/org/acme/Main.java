package org.acme;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

@QuarkusMain
public class Main implements QuarkusApplication {

    @Inject
    Sequencer sequencer;

    public static void main(String... args) {
        Quarkus.run(Main.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        Vertx vertx = Vertx.vertx();
        Router router = Router.router(vertx);
        Config config = ConfigProvider.getConfig();

        Quarkus.waitForExit();
        return 0;
    }
}
