package io.quarkus.ext.debezium;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkus.ext.debezium.runtime.DebeziumKey;
import io.quarkus.ext.debezium.runtime.DebeziumValue;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
//@Disabled
public class DebeziumTest {
	static final protected Logger log = LoggerFactory.getLogger(DebeziumTest.class);

	@Order(1)
	@BeforeAll
	static public void beforeAll() {
		log.debug("...");
	}

	@AfterAll
	static public void afterAll() {
		log.debug("...");
	}

	@Order(10)
	@RegisterExtension
	static final QuarkusUnitTest config = new QuarkusUnitTest() //
			.setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class) //
					.addAsResource("application.properties", "application.properties") //
					.addClasses(TestBean.class) //
					.addClasses(DemoConverter.class, DemoGenerator.class, DemoReceiver.class));

	@Inject
	TestBean testBean;

	// @BeforeEach
	// public void setUp() throws Exception {
	// LOGGER.debug("setUp...");
	// }
	//
	@AfterEach
	public void tearDown() throws Exception {
		log.debug("tearDown...");
	}

	@Test
	public void test() {
		log.info("BEGIN test...");
		try {
			CountDownLatch latch = new CountDownLatch(1);
			new Thread(() -> {
				for (int i = 0; i < 500000; i++) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
				}
				latch.countDown();
			}).start();

			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			log.error("", e);
		} finally {
			log.info("END test.");
		}
	}

	@ApplicationScoped
	static public class TestBean { // must public

		@Inject
		@io.smallrye.reactive.messaging.annotations.Stream("write-to-next2")
		io.smallrye.reactive.messaging.annotations.Emitter<String> emitter;

		@PostConstruct
		void onPostConstruct() {
			log.debug("onPostConstruct");
		}

		@PreDestroy
		void onPreDestroy() {
			log.debug("onPreDestroy");
		}

		/**
		 * Called when the runtime has started
		 *
		 * @param event
		 */
		void onStart(@Observes StartupEvent event) {
			log.debug("onStart, event={}", event);
		}

		void onStop(@Observes ShutdownEvent event) {
			log.debug("onStop, event={}", event);
		}

		/**
		 * Kafka topic
		 * 
		 * @see io.smallrye.reactive.messaging.kafka.ReceivedKafkaMessage
		 */
		@Incoming("customers")
		@Outgoing("customers-next")
		@Broadcast
		public ObjectNode process(KafkaMessage<SchemaAndValue, SchemaAndValue> msg) { //
			log.debug("msg.getTopic: {}", msg.getTopic());
			log.debug("msg.getKey: {}", msg.getKey());
			log.debug("msg.getPayload: {}", msg.getPayload());
			log.debug("msg.getHeaders: {}", msg.getHeaders());

			ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
			rootNode.put("topic", msg.getTopic());

			//
			ObjectNode headerNode = JsonNodeFactory.instance.objectNode();
			msg.getHeaders().unwrap().forEach(header -> {
				// log.debug("class: {}, header.key: {}, value: {}, toString: {}",
				// header.getClass().getName(),
				// header.key(), header.value(), header.toString());
				put(headerNode, header.key(), header.value());
			});
			if (headerNode.fields().hasNext()) {
				rootNode.set("header", headerNode);
			}

			//
			SchemaAndValue key = msg.getKey();
			DebeziumKey schemaKey = new DebeziumKey(key);

			schemaKey.getAllValues().ifPresent(fields -> {
				ObjectNode keyNode = rootNode.putObject("key");
				Stream.of(fields).forEach(fv -> {
					String name = fv.name();
					Object val = fv.getValue();
					// log.debug("## key name: {}, val: {}", name, val);
					put(keyNode, name, val);
				});
			});

			SchemaAndValue payload = msg.getPayload();
			if (payload == null) {
				log.info("(payload == null)"); // after deleted record action will send addon null payload message
//                log.info("msg.getTopic: {}", msg.getTopic());
//                log.info("msg.getKey: {}", msg.getKey());
//                log.info("msg.getPayload: {}", msg.getPayload());
				return rootNode;
			}
			DebeziumValue schemaPayload = new DebeziumValue(payload);

			rootNode.put("op", schemaPayload.getOp().toString());
			if (schemaPayload.isUpdated()) {
				ObjectNode opNode = rootNode.putObject("updated");
				Stream.of(schemaPayload.getUpdatedFields()).forEach(updated -> {
					String name = updated.name();
					Object val = updated.getAfter().getValue();
					put(opNode, name, val);
					// log.debug("## isUpdated name: {}, val: {}", name, val);
				});
			} else if (schemaPayload.isCreated()) {
				ObjectNode opNode = rootNode.putObject("created");
				Stream.of(schemaPayload.getCreatedFields()).forEach(created -> {
					String name = created.name();
					Object val = created.getValue();
					put(opNode, name, val);
					// log.debug("## isCreated name: {}, val: {}", name, val);
				});
			} else if (schemaPayload.isDeleted()) {
				ObjectNode opNode = rootNode.putObject("deleted");
				Stream.of(schemaPayload.getDeletedFields()).forEach(deleted -> {
					String name = deleted.name();
					Object val = deleted.getValue();
					put(opNode, name, val);
					// log.debug("## isDeleted name: {}, val: {}", name, val);
				});
			} else {
				log.info("schemaPayload.op: {}", schemaPayload.getOp());
			}

			// log.debug("rootNode: {}", rootNode);
			return rootNode;
		}

		/**
		 * 
		 */
		@Incoming("customers-next")
		public void process(ObjectNode json) {
			try {
				while (!emitter.isRequested()) {
					log.debug("emitter.isRequested: {}", emitter.isRequested());
					Thread.sleep(10);
				}
				if (json == null) {
					// Thread.dumpStack();
					log.warn("(json == null)");
				} else {
					log.debug("send json: {}", json);
					emitter.send(json.toString());
				}
			} catch (Throwable e) {
				log.error("", e);
				throw new RuntimeException(e);
			}
		}

		private ObjectNode put(ObjectNode node, String name, Object value) {
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
			} else if (value instanceof String[]) { // class net.minidev.json.JSONObject
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
				node.set(name, put(map));
			} else {
				node.put(name, value.toString());
			}
			return node;
		}

		private ArrayNode addElement(ArrayNode node, Object value) {
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
			} else if (value instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, ?> map = (Map<String, ?>) value;
				node.add(put(map));
			} else {
				node.add(value.toString());
			}
			return node;
		}

		private ObjectNode put(Map<String, ?> map) {
			ObjectNode node = JsonNodeFactory.instance.objectNode();
			for (String key : map.keySet()) {
				put(node, key, map.get(key));
			}
			return node;
		}
	}

}
