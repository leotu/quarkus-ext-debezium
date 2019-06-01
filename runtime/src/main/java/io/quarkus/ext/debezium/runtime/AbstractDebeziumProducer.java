package io.quarkus.ext.debezium.runtime;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.jboss.logging.Logger;

import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * Produces Debezium Message
 * <p/>
 * https://smallrye.io/smallrye-reactive-messaging/#_outgoing
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public abstract class AbstractDebeziumProducer {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractDebeziumProducer.class);

    @SuppressWarnings("unused")
    private DebeziumConfig debeziumConfig;

    public DebeziumMessage transform(KafkaMessage<SchemaAndValue, SchemaAndValue> kafkaMsg) {
        return new DebeziumMessage(kafkaMsg);
    }

    /**
     *
     */
    public void setDebeziumConfig(DebeziumConfig debeziumConfig) {
        this.debeziumConfig = debeziumConfig;
    }

    /**
     * CDI: Ambiguous dependencies
     */
    @Target({ ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.TYPE })
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Qualifier
    static public @interface DebeziumQualifier {

        String value();
    }
}
