package io.quarkus.ext.debezium;

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

            ObjectNodeHelper helper = new ObjectNodeHelper();
            ObjectNode rootNode = helper.getRoot();
            rootNode.put("topic", msg.getTopic());

            //
            ObjectNode headerNode = JsonNodeFactory.instance.objectNode();
            msg.getHeaders().unwrap().forEach(header -> {
                // log.debug("class: {}, header.key: {}, value: {}, toString: {}",
                // header.getClass().getName(),
                // header.key(), header.value(), header.toString());
                helper.put(headerNode, header.key(), header.value());
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
                    helper.put(keyNode, name, val);
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
                    helper.put(opNode, name, val);
                    // log.debug("## isUpdated name: {}, val: {}", name, val);
                });
            } else if (schemaPayload.isCreated()) {
                ObjectNode opNode = rootNode.putObject("created");
                Stream.of(schemaPayload.getCreatedFields()).forEach(created -> {
                    String name = created.name();
                    Object val = created.getValue();
                    helper.put(opNode, name, val);
                    // log.debug("## isCreated name: {}, val: {}", name, val);
                });
            } else if (schemaPayload.isDeleted()) {
                ObjectNode opNode = rootNode.putObject("deleted");
                Stream.of(schemaPayload.getDeletedFields()).forEach(deleted -> {
                    String name = deleted.name();
                    Object val = deleted.getValue();
                    helper.put(opNode, name, val);
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
    }

}
