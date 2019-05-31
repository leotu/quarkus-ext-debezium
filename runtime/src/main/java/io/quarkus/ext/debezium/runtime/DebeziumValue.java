package io.quarkus.ext.debezium.runtime;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * 
 * @see org.apache.kafka.connect.data.ConnectSchema
 * @see io.debezium.connector.mysql.SourceInfo
 * @see io.debezium.connector.postgresql.SourceInfo
 * @see io.debezium.connector.sqlserver.SourceInfo
 * @see io.debezium.connector.mongodb.SourceInfo
 * @see io.smallrye.reactive.messaging.kafka.ReceivedKafkaMessage
 */
public class DebeziumValue {
    static final protected Logger log = LoggerFactory.getLogger(DebeziumValue.class);

    final private SchemaAndValue schemaAndValue;
    final private Struct payload;
    final private Operation op;
    final private Date eventTime;

    public DebeziumValue(KafkaMessage<?, SchemaAndValue> msg) {
        this(msg.getPayload());
    }

    public DebeziumValue(SchemaAndValue schemaAndValue) {
        this.schemaAndValue = schemaAndValue;
        this.payload = (Struct) this.schemaAndValue.value();
        String opType = (String) this.payload.get(FieldName.OPERATION);
        this.op = Operation.forCode(opType);
        Long timestamp = (Long) this.payload.get(FieldName.TIMESTAMP);
        this.eventTime = new Date(timestamp);
    }

    public Operation getOp() {
        return op;
    }

    public boolean isUpdated() {
        return op == Operation.UPDATE;
    }

    public boolean isCreated() {
        return op == Operation.CREATE;
    }

    public boolean isDeleted() {
        return op == Operation.DELETE;
    }

    public boolean isRead() {
        return op == Operation.READ;
    }

    public Date getEventTime() {
        return eventTime;
    }

    // public List<Field> getBeforeFields() {
    // return schemaAndValue.schema().field(FieldName.BEFORE).schema().fields();
    // }
    //
    // public List<Field> getAfterFields() {
    // return schemaAndValue.schema().field(FieldName.AFTER).schema().fields();
    // }
    //
    // public Field getBeforeField(String fieldName) {
    // return
    // schemaAndValue.schema().field(FieldName.BEFORE).schema().field(fieldName);
    // }
    //
    // public Field getAfterField(String fieldName) {
    // return
    // schemaAndValue.schema().field(FieldName.AFTER).schema().field(fieldName);
    // }
    //
    // public Type getFieldType(Field field) {
    // return field.schema().type();
    // }
    //
    // public boolean isOptional(Field field) {
    // return field.schema().isOptional();
    // }
    //
    // public Object getDefaultValue(Field field) {
    // return field.schema().defaultValue();
    // }
    //
    // /**
    // * Without default value
    // */
    // public Optional<Object> getBeforeValue(String fieldName) {
    // Object before = payload.get(FieldName.BEFORE);
    // if (before == null) {
    // return Optional.empty();
    // }
    // return Optional.ofNullable(getValue((Struct) before, fieldName));
    // }
    //
    // /**
    // * Without default value
    // */
    // public Optional<Object> getAfterValue(String fieldName) {
    // Object after = payload.get(FieldName.AFTER);
    // if (after == null) {
    // return Optional.empty();
    // }
    // return Optional.ofNullable(getValue((Struct) after, fieldName));
    // }

    static public class UpdatedField {
        private final FieldValue before;
        private final FieldValue after;

        public UpdatedField(FieldValue before, FieldValue after) {
            this.before = before;
            this.after = after;
        }

        public FieldValue getBefore() {
            return before;
        }

        public FieldValue getAfter() {
            return after;
        }

        public String name() {
            return before.name();
        }

        @Override
        public String toString() {
            return super.toString() + "{before:" + before + ", after:" + after + "}";
        }

    }

    static public class FieldValue {
        private final Field field;
        private final Object value;

        public FieldValue(Field field, Object value) {
            this.field = field;
            this.value = value;
        }

        public Field getField() {
            return field;
        }

        public Object getValue() {
            return value;
        }

        public String name() {
            return field.name();
        }

        @Override
        public String toString() {
            return super.toString() + "{field:" + field.name() + ", value:" + value + "}";
        }
    }

    /**
     * @return never null
     */
    public UpdatedField[] getUpdatedFields() {
        if (op != Operation.UPDATE) {
            throw new IllegalStateException("Not Updated Action");
        }

        Struct beforeRecord = (Struct) payload.get(FieldName.BEFORE);
        Struct afterRecord = (Struct) payload.get(FieldName.AFTER);
        if (beforeRecord == null || afterRecord == null) {
            throw new IllegalStateException("(beforeRecord == null || afterRecord == null)");
        }

        if (beforeRecord.schema().fields().size() != afterRecord.schema().fields().size()) {
            throw new IllegalStateException(
                    "(beforeRecord.schema().fields().size() != afterRecord.schema().fields().size())");
        }
        // Map<String, UpdatedField> updatedFields = new
        // TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<UpdatedField> updatedFields = new ArrayList<>();
        beforeRecord.schema().fields().forEach(beforeField -> {
            String fieldName = beforeField.name();
            Object beforeValue = getValue(beforeRecord, fieldName);
            Object afterValue = getValue(afterRecord, fieldName);
            if (!Objects.equals(beforeValue, afterValue)) {
                FieldValue before = new FieldValue(beforeField, beforeValue);
                FieldValue after = new FieldValue(beforeRecord.schema().field(fieldName), afterValue);
                UpdatedField updated = new UpdatedField(before, after);
                updatedFields.add(updated);
            }
        });

        if (updatedFields.isEmpty()) {
            throw new IllegalStateException("Not Updated Field");
        }
        return updatedFields.toArray(new UpdatedField[0]);
    }

    /**
     * @return never null
     */
    public FieldValue[] getCreatedFields() {
        if (op != Operation.CREATE) {
            throw new IllegalStateException("Not Created Action");
        }

        Struct beforeRecord = (Struct) payload.get(FieldName.BEFORE);
        Struct afterRecord = (Struct) payload.get(FieldName.AFTER);
        if (beforeRecord != null || afterRecord == null) {
            throw new IllegalStateException("(beforeRecord != null || afterRecord == null)");
        }

        FieldValue[] createdFields = getAllAfterValues().get();
        if (createdFields.length == 0) {
            throw new IllegalStateException("Not Created Field");
        }
        return createdFields;
    }

    /**
     * @return never null
     */
    public FieldValue[] getDeletedFields() {
        if (op != Operation.DELETE) {
            throw new IllegalStateException("Not Deleted Action");
        }

        Struct beforeRecord = (Struct) payload.get(FieldName.BEFORE);
        Struct afterRecord = (Struct) payload.get(FieldName.AFTER);
        if (beforeRecord == null || afterRecord != null) {
            throw new IllegalStateException("(beforeRecord == null || afterRecord != null)");
        }

        FieldValue[] deletedFields = getAllBeforeValues().get();
        if (deletedFields.length == 0) {
            throw new IllegalStateException("Not Deleted Field");
        }
        return deletedFields;
    }

    /**
     * Without default value
     */
    public Optional<FieldValue[]> getAllBeforeValues() {
        Object before = payload.get(FieldName.BEFORE);
        if (before == null) {
            return Optional.empty();
        }

        Struct record = (Struct) before;
        List<Field> fields = record.schema().fields();
        FieldValue fieldValueAry[] = new FieldValue[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldValueAry[i] = new FieldValue(field, getValue(record, field.name()));
        }

        return Optional.of(fieldValueAry);
    }

    /**
     * Without default value
     */
    public Optional<FieldValue[]> getAllAfterValues() {
        Object after = payload.get(FieldName.AFTER);
        if (after == null) {
            return Optional.empty();
        }

        Struct record = (Struct) after;
        List<Field> fields = record.schema().fields();
        FieldValue fieldValueAry[] = new FieldValue[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldValueAry[i] = new FieldValue(field, getValue(record, field.name()));
        }

        return Optional.of(fieldValueAry);
    }

    /**
     * Without default value
     */
    static Object getValue(Struct record, String fieldName) {
        Field field = record.schema().field(fieldName);
        if (field == null) {
            log.warn("(field == null), fieldName: {}", fieldName);
            throw new IllegalArgumentException("Field: '" + fieldName + "' Not Found");
        }
        Type type = field.schema().type();
        String typeLogicalName = field.schema().name();

        Object val;
        if (type == Schema.Type.INT8) {
            val = record.getInt8(field.name());
        } else if (type == Schema.Type.INT16) {
            val = record.getInt16(field.name());
        } else if (type == Schema.Type.INT32) {
            // 8200 = '1992-06-14', 15139 = '2011-06-14'
            if (io.debezium.time.Date.SCHEMA_NAME.equals(typeLogicalName)) {
                Integer epochDay = record.getInt32(field.name());
                val = epochDay == null ? null : LocalDate.ofEpochDay(epochDay);
            } else {
                val = record.getInt32(field.name());
            }
        } else if (type == Schema.Type.INT64) {
            // 349891365000 = '2019-05-31 10:27:01'
            if (io.debezium.time.Timestamp.SCHEMA_NAME.equals(typeLogicalName)) {
                Long tm = record.getInt64(field.name());
                val = tm == null ? null : LocalDateTime.ofInstant(new Date(tm).toInstant(), ZoneOffset.UTC);
            } else {
                val = record.getInt64(field.name());
            }
        } else if (type == Schema.Type.BOOLEAN) {
            val = record.getBoolean(field.name());
        } else if (type == Schema.Type.STRING) {
            // "2019-05-31T02:27:01Z" ==> "2019-05-31T10:27:01"
            if (io.debezium.time.ZonedTimestamp.SCHEMA_NAME.equals(typeLogicalName)) {
                String iso8601Str = record.getString(field.name());
                val = iso8601Str == null ? null
                        : LocalDateTime.ofInstant(
                                ZonedDateTime.parse(iso8601Str, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant(),
                                ZoneId.systemDefault());
            } else {
                val = record.getString(field.name());
            }
        } else if (type == Schema.Type.FLOAT32) {
            val = record.getFloat32(field.name());
        } else if (type == Schema.Type.FLOAT64) {
            val = record.getFloat64(field.name());
        } else if (type == Schema.Type.BYTES) {
            if (Decimal.LOGICAL_NAME.equals(typeLogicalName)) {
                val = getBigDecimal(record, field.name());
            } else {
                val = record.getBytes(field.name());
            }
        } else if (type == Schema.Type.ARRAY) {
            val = record.getArray(field.name());
        } else if (type == Schema.Type.MAP) {
            val = record.getMap(field.name());
        } else if (type == Schema.Type.STRUCT) {
            val = record.getStruct(field.name());
        } else {
            val = record.getWithoutDefault(field.name());
            log.warn("No match: fieldName: {}, type: {}, typeLogicalName: {}, val.class: {}, val: {}", fieldName, type,
                    typeLogicalName, val == null ? "<null>" : val.getClass().getName(), val);
        }
        //        log.debug("fieldName: {}, type: {}, typeLogicalName: {}, val.class: {}, val: {}", fieldName, type,
        //                typeLogicalName, val == null ? "<null>" : val.getClass().getName(), (val instanceof byte[]) ? "<byte[]>" : val);
        return val;
    }

    /**
     * Without default value
     */
    static protected BigDecimal getBigDecimal(Struct record, String fieldName) {
        return (BigDecimal) record.getWithoutDefault(fieldName);
    }

    /**
     * With default value
     */
    static protected Object getWithDefault(Struct record, String fieldName) {
        return record.get(fieldName);
    }

}
