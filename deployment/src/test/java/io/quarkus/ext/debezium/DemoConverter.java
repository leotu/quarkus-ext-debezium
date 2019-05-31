package io.quarkus.ext.debezium;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.annotations.Broadcast;

/**
 * <pre>
 * [demo-topic] is Kafka topic
 * [write-to-next] is "in-memory" stream
 * </pre>
 */
@ApplicationScoped
public class DemoConverter {
    static final protected Logger log = LoggerFactory.getLogger(DemoGenerator.class);

    private static final double CONVERSION_RATE = 0.88;

    @Incoming("read-from")
    @Outgoing("write-to-next")
    @Broadcast
    public double process(int input) {
        log.debug("<< input: {}", input);
        return input * CONVERSION_RATE;
    }

}
