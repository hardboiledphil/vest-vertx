package com.acme;

import io.quarkus.vertx.ConsumeEvent;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class GreetingService {

    @ConsumeEvent("greetings")
    public String greeting(String name) {
        return "hello " + name;
    }
}
