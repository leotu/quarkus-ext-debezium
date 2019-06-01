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
 * KafkaMessage.getPayload()
 * 
 * @see org.apache.kafka.common.serialization.StringSerializer
 * @see org.apache.kafka.common.serialization.StringDeserializer
 * @see io.vertx.kafka.client.serialization.JsonObjectDeserializer
 * @see io.debezium.examples.graphql.serdes.ChangeEventAwareJsonSerde
 * @see org.apache.kafka.connect.json.JsonSchema
 * @see io.vertx.kafka.client.serialization.*;
 */
@RegisterForReflection
public class DebeziumDataValueDeserializer implements Deserializer<SchemaAndValue> {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumDataValueDeserializer.class);

    private boolean debug = log.isDebugEnabled();

    private final JsonConverter jsonConverter;

    public DebeziumDataValueDeserializer() {
        this.jsonConverter = new JsonConverter();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (isKey) {
            throw new IllegalArgumentException("is key)");
        }
        if (debug) {
            log.debug("isKey: {}, configs: {}", isKey, configs);
        }
        jsonConverter.configure(configs, isKey);
    }

    @Override
    public SchemaAndValue deserialize(String topic, byte[] data) {
        if (data == null) {
            log.warn("(data == null), topic: {}", topic);
            return null;
        }
        if (debug) {
            log.debug("topic: {}, data.length: {}, data: {}", topic, data.length, new String(data));
        }
        try {
            SchemaAndValue schemaAndValue = jsonConverter.toConnectData(topic, data);
            if (debug) {
                byte[] data2 = jsonConverter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
                if (data.length != data2.length) {
                    log.warn("(data.length != data2.length), topic: {}, data.length: {}, data2.length: {}, data2: {}",
                            topic, data.length, data2.length, new String(data2));
                }
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
