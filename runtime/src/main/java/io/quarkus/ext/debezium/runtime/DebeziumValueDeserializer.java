package io.quarkus.ext.debezium.runtime;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import home.leo.app.InitializeApp.JsonbConverter;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * 
 */
@RegisterForReflection
public class DebeziumValueDeserializer implements Deserializer<DebeziumValue> {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumValueDeserializer.class);

    private final DebeziumDataValueDeserializer delegate;

    public DebeziumValueDeserializer() {
        this.delegate = new DebeziumDataValueDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public DebeziumValue deserialize(String topic, byte[] data) {
        SchemaAndValue schemaAndValue = delegate.deserialize(topic, data);
        if (schemaAndValue == null) {
            log.info("(schemaAndValue == null), topic: {}", topic);
            return null;
        }
        return new DebeziumValue(schemaAndValue);
    }

    @Override
    public void close() {
        this.delegate.close();
    }

}
