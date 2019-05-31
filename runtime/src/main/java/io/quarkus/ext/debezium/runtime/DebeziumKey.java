package io.quarkus.ext.debezium.runtime;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.ext.debezium.runtime.DebeziumValue.FieldValue;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * 
 * @see io.smallrye.reactive.messaging.kafka.ReceivedKafkaMessage
 */
public class DebeziumKey {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumKey.class);

    final private SchemaAndValue schemaAndValue;
    final private Struct payload;

    public DebeziumKey(KafkaMessage<SchemaAndValue, ?> msg) {
        this(msg.getKey());
    }

    public DebeziumKey(SchemaAndValue schemaAndValue) {
        this.schemaAndValue = schemaAndValue;
        this.payload = (Struct) this.schemaAndValue.value();
    }

    public boolean isEmpty() {
        return payload.schema().fields().isEmpty();
    }

    public List<Field> getFields() {
        return payload.schema().fields();
    }

    //	public Type getFieldType(Field field) {
    //		return field.schema().type();
    //	}
    //
    //	public boolean isOptional(Field field) {
    //		return field.schema().isOptional();
    //	}
    //
    //	public Object getDefaultValue(Field field) {
    //		return field.schema().defaultValue();
    //	}
    //
    //	/**
    //	 * Without default value
    //	 */
    //	public Optional<Object> getValue(Field field) {
    //		return getValue(field.name());
    //	}
    //
    //	/**
    //	 * Without default value
    //	 */
    //	public Optional<Object> getValue(String fieldName) {
    //		Object val = payload.get(fieldName);
    //		if (val == null) {
    //			return Optional.empty();
    //		}
    //		return Optional.ofNullable(DebeziumValue.getValue((Struct) val, fieldName));
    //	}

    /**
     * Without default value
     */
    public Optional<FieldValue[]> getAllValues() {
        List<Field> fields = payload.schema().fields();
        if (fields.isEmpty()) {
            return Optional.empty();
        }

        FieldValue fieldValueAry[] = new FieldValue[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldValueAry[i] = new FieldValue(field, DebeziumValue.getValue(payload, field.name()));
        }
        return Optional.of(fieldValueAry);
    }

}
