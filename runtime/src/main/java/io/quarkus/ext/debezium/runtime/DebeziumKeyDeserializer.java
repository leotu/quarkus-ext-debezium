package io.quarkus.ext.debezium.runtime;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
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
public class DebeziumKeyDeserializer implements Deserializer<SchemaAndValue> {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumKeyDeserializer.class);

    private final JsonConverter jsonConverter;

    public DebeziumKeyDeserializer() {
        this.jsonConverter = new JsonConverter();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.debug("isKey: {}, configs: {}", isKey, configs);
        jsonConverter.configure(configs, isKey);
    }

    @Override
    public SchemaAndValue deserialize(String topic, byte[] data) {
        if (data == null) {
            log.info("(data == null), topic: {}", topic);
            return null;
        }
        log.debug("topic: {}", topic);
        log.debug("data.length: {}, data: {}", data.length, new String(data));
        try {
            SchemaAndValue schemaAndValue = jsonConverter.toConnectHeader(topic, null, data);

            byte[] data2 = jsonConverter.fromConnectHeader(topic, null, schemaAndValue.schema(), schemaAndValue.value());
            if (data.length != data2.length) {
                log.warn("(data.length != data2.length), data.length: {}, data2.length: {}, data2: {}", data.length,
                        data2.length, new String(data2));
            }
            return schemaAndValue;
        } catch (Throwable e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

}
