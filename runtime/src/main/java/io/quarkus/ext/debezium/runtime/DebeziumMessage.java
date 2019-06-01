package io.quarkus.ext.debezium.runtime;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.smallrye.reactive.messaging.kafka.KafkaMessage;
import io.smallrye.reactive.messaging.kafka.MessageHeaders;

/**
 * 
 * @see io.smallrye.reactive.messaging.kafka.ReceivedKafkaMessage
 */
public class DebeziumMessage { // implements KafkaMessage<DebeziumKey, DebeziumValue> {

	final private DebeziumKey key;
	final private DebeziumValue payload;
	final private KafkaMessage<SchemaAndValue, SchemaAndValue> kafkaMsg;

	public DebeziumMessage(KafkaMessage<SchemaAndValue, SchemaAndValue> kafkaMsg) {
		this.key = new DebeziumKey(kafkaMsg);
		this.payload = kafkaMsg.getPayload() == null ? null : new DebeziumValue(kafkaMsg);
		this.kafkaMsg = kafkaMsg;
	}

	// @Override
	public DebeziumKey getKey() {
		return key;
	}

	// @Override
	public DebeziumValue getPayload() {
		return payload;
	}

	// @Override
	public String getTopic() {
		return kafkaMsg.getTopic();
	}

	// @Override
	public MessageHeaders getHeaders() {
		return kafkaMsg.getHeaders();
	}

	// @Override
	public Integer getPartition() {
		return kafkaMsg.getPartition();
	}

	// @Override
	public Long getTimestamp() {
		return kafkaMsg.getTimestamp();
	}

	public CompletionStage<Void> ack() {
		return kafkaMsg.ack();
	}

}
