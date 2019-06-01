package io.quarkus.ext.debezium.runtime;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import home.leo.app.InitializeApp.JsonbConverter;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * KafkaMessage.getKey()
 * 
 * @see org.apache.kafka.common.serialization.StringSerializer
 * @see org.apache.kafka.common.serialization.StringDeserializer
 * @see io.vertx.kafka.client.serialization.JsonObjectDeserializer
 * @see io.debezium.examples.graphql.serdes.ChangeEventAwareJsonSerde
 * @see org.apache.kafka.connect.json.JsonSchema
 * @see io.vertx.kafka.client.serialization.*;
 */
@RegisterForReflection
public class DebeziumKeyDeserializer implements Deserializer<DebeziumKey> {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumKeyDeserializer.class);

    private final DebeziumDataKeyDeserializer delegate;

    public DebeziumKeyDeserializer() {
        this.delegate = new DebeziumDataKeyDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public DebeziumKey deserialize(String topic, byte[] data) {
        SchemaAndValue schemaAndValue = delegate.deserialize(topic, data);
        if (schemaAndValue == null) {
            log.info("(schemaAndValue == null), topic: {}", topic);
            return null;
        }
        return new DebeziumKey(schemaAndValue);
    }

    @Override
    public void close() {
        this.delegate.close();
    }

}
