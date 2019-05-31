package io.quarkus.ext.debezium;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [write-to-next] is "in-memory" stream
 */
@ApplicationScoped
public class DemoReceiver {

    static final protected Logger log = LoggerFactory.getLogger(DemoReceiver.class);

    @Incoming("write-to-next")
    public void process(Double received) {
        log.debug("** received: {}", received);
    }

    @Incoming("write-to-next2")
    public void process(String received) {
        log.debug("** received: {}", received);
    }
}
