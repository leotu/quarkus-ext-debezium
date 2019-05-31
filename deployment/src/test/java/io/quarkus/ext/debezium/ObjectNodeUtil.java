package io.quarkus.ext.debezium;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class ObjectNodeUtil {

    private final ObjectNode root;

    public ObjectNodeUtil() {
        this.root = JsonNodeFactory.instance.objectNode();
    }

    public ObjectNode getRoot() {
        return root;
    }

    public ObjectNode put(ObjectNode node, String name, Object value) {
        if (value == null) {
            node.putNull(name);
        } else if (value instanceof Integer) {
            node.put(name, (Integer) value);
        } else if (value instanceof String) {
            node.put(name, (String) value);
        } else if (value instanceof Boolean) {
            node.put(name, (Boolean) value);
        } else if (value instanceof Date) {
            node.put(name, ((Date) value).getTime());
        } else if (value instanceof Long) {
            node.put(name, (Long) value);
        } else if (value instanceof Double) {
            node.put(name, (Double) value);
        } else if (value instanceof Float) {
            node.put(name, (Float) value);
        } else if (value instanceof BigDecimal) {
            node.put(name, (BigDecimal) value);
        } else if (value instanceof BigInteger) {
            node.put(name, new BigDecimal((BigInteger) value));
        } else if (value instanceof Byte) {
            node.put(name, (Byte) value);
        } else if (value instanceof byte[]) {
            node.put(name, (byte[]) value);
        } else if (value instanceof char[]) {
            node.put(name, new String((char[]) value));
        } else if (value instanceof ObjectNode) {
            node.set(name, (ObjectNode) value);
        } else if (value instanceof JsonNode) {
            node.set(name, (JsonNode) value);
        } else if (value instanceof String[]) {
            String[] strs = (String[]) value;
            ArrayNode arrayNode = node.putArray(name);
            for (String str : strs) {
                arrayNode.add(str);
            }
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            ArrayNode arrayNode = node.putArray(name);
            for (Object obj : list) {
                addElement(arrayNode, obj);
            }
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) value;
            put(node.putObject(name), map);
        } else {
            node.put(name, value.toString());
        }
        return node;
    }

    public ArrayNode addElement(ArrayNode node, Object value) {
        if (value == null) {
            node.addNull();
        } else if (value instanceof Integer) {
            node.add((Integer) value);
        } else if (value instanceof String) {
            node.add((String) value);
        } else if (value instanceof Boolean) {
            node.add((Boolean) value);
        } else if (value instanceof Date) {
            node.add(((Date) value).getTime());
        } else if (value instanceof Long) {
            node.add((Long) value);
        } else if (value instanceof Double) {
            node.add((Double) value);
        } else if (value instanceof Float) {
            node.add((Float) value);
        } else if (value instanceof BigDecimal) {
            node.add((BigDecimal) value);
        } else if (value instanceof BigInteger) {
            node.add(new BigDecimal((BigInteger) value));
        } else if (value instanceof Byte) {
            node.add((Byte) value);
        } else if (value instanceof byte[]) {
            node.add((byte[]) value);
        } else if (value instanceof char[]) {
            node.add(new String((char[]) value));
        } else if (value instanceof ObjectNode) {
            node.add((ObjectNode) value);
        } else if (value instanceof JsonNode) {
            node.add((JsonNode) value);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            ArrayNode arrayNode = node.addArray();
            for (Object obj : list) {
                addElement(arrayNode, obj);
            }
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) value;
            put(node.objectNode(), map);
        } else {
            node.add(value.toString());
        }
        return node;
    }

    public ObjectNode put(ObjectNode node, Map<String, ?> map) {
        for (String key : map.keySet()) {
            put(node, key, map.get(key));
        }
        return node;
    }
}
