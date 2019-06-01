package io.quarkus.ext.debezium;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;

/**
 * [demo-topic] is Kafka topic
 */
@ApplicationScoped
public class DemoGenerator {
    static final protected Logger log = LoggerFactory.getLogger(DemoGenerator.class);

    private Random random = new Random();

    @Outgoing("write-to")
    public Flowable<Integer> generate() {
        return Flowable.interval(50, TimeUnit.SECONDS).map(tick -> {
            int value = random.nextInt(100);
            log.debug(">> tick: {}, value: {}", tick, value);
            return value;
        });
    }

}
