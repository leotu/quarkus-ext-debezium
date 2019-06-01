package io.quarkus.ext.debezium;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
// import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
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

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.quarkus.ext.debezium.runtime.DebeziumKey;
import io.quarkus.ext.debezium.runtime.DebeziumMessage;
import io.quarkus.ext.debezium.runtime.DebeziumValue;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * https://smallrye.io/smallrye-reactive-messaging/#_reactive_streams_reactive_stream_operators_rx_java
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

    // @Order(10)
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

        // @Inject
        // @io.smallrye.reactive.messaging.annotations.Stream("write-to-next2")
        // io.smallrye.reactive.messaging.annotations.Emitter<String> emitter;

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

        @Incoming("customers")
        @Outgoing("customers-next")
        // @Broadcast
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Message<ObjectNode>> process(KafkaMessage<DebeziumKey, DebeziumValue> msg) {
            log.debug("msg.getTopic: {}", msg.getTopic());
            log.debug("msg.getKey: {}", msg.getKey());
            log.debug("msg.getPayload: {}", msg.getPayload());
            log.debug("msg.getHeaders: {}", msg.getHeaders());
            log.debug("msg.getTimestamp: {}",
                    LocalDateTime.ofInstant(new Date(msg.getTimestamp()).toInstant(), ZoneId.systemDefault()));

            ObjectNodeHelper helper = new ObjectNodeHelper();
            ObjectNode rootNode = helper.createObjectNode();
            rootNode.put("topic", msg.getTopic());

            //
            ObjectNode headerNode = helper.createObjectNode();
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
            DebeziumKey key = msg.getKey();

            key.getAllValues().ifPresent(fields -> {
                ObjectNode keyNode = rootNode.putObject("key");
                Stream.of(fields).forEach(fv -> {
                    String name = fv.name();
                    Object val = fv.getValue();
                    // log.debug("## key name: {}, val: {}", name, val);
                    helper.put(keyNode, name, val);
                });
            });

            DebeziumValue value = msg.getPayload();
            if (value == null) {
                log.info("(value == null)"); // after deleted record action will send addon null payload message
                return manualAck(msg, rootNode);
            }

            log.debug("value.cdcEventTime: {}", value.getCdcEventTime());

            rootNode.put("op", value.getOp().toString());
            if (value.isUpdated()) {
                ObjectNode opNode = rootNode.putObject("updated");
                ObjectNode afterNode = opNode.putObject("payload");
                Stream.of(value.getAllAfterValues().get()).forEach(after -> {
                    helper.put(afterNode, after.name(), after.getValue());
                });

                ObjectNode changedNode = opNode.putObject("changed");
                Stream.of(value.getUpdatedFields()).forEach(updated -> {
                    String name = updated.name();
                    ObjectNode valueNode = changedNode.putObject(name);
                    helper.put(valueNode, "before", updated.getBefore().getValue());
                    helper.put(valueNode, "_after", updated.getAfter().getValue());
                });
            } else if (value.isCreated()) {
                ObjectNode opNode = rootNode.putObject("created");
                Stream.of(value.getCreatedFields()).forEach(created -> {
                    String name = created.name();
                    Object val = created.getValue();
                    helper.put(opNode, name, val);
                });
            } else if (value.isDeleted()) {
                ObjectNode opNode = rootNode.putObject("deleted");
                Stream.of(value.getDeletedFields()).forEach(deleted -> {
                    String name = deleted.name();
                    Object val = deleted.getValue();
                    helper.put(opNode, name, val);
                });
            } else {
                log.info("value.op: {}", value.getOp());
            }

            return manualAck(msg, rootNode);
        }

        public CompletionStage<Message<ObjectNode>> manualAck(KafkaMessage<DebeziumKey, DebeziumValue> msg,
                ObjectNode rootNode) {
            return CompletableFuture //
                    .supplyAsync(() -> {
                        log.debug("supplyAsync, rootNode: {}", rootNode);
                        // if (true)
                        // throw new RuntimeException("supplyAsync error");
                        return rootNode;
                    }) //
                    .thenApply(payload -> { // Message::of
                        log.debug("thenApply, payload: {}", payload);
                        Message<ObjectNode> m = Message.of(payload);
                        log.debug("thenApply, m: {}", m);
                        // if (true)
                        // throw new RuntimeException("thenApply error");
                        return m;
                    }) //
                    .thenCompose(m -> {
                        log.debug("thenCompose, m: {}", m);
                        // if (true)
                        // throw new RuntimeException("thenCompose error");
                        return m.ack() //
                                .thenApply(x -> {
                                    log.debug("thenCompose, thenApply, msg: {}", msg);
                                    // if (true)
                                    // throw new RuntimeException("thenCompose thenApply error");
                                    msg.ack();
                                    return m;
                                });
                    }).exceptionally(err -> {
                        log.error("exceptionally", err);
                        return null;
                    });
        }

        // public CompletionStage<Message<Void>> manualAck(Message<ObjectNode> msg,
        // ObjectNode rootNode) {
        // return CompletableFuture.supplyAsync(() -> null).thenApply(Message::of)
        // .thenCompose(m -> m.ack().thenApply(x -> m)); //
        // CompletableFuture.completedFuture(null);
        // }

        /**
         * 
         */
        @Incoming("customers-next")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Void> processNext(Message<ObjectNode> msg) {
            ObjectNode json = msg.getPayload();

            try {
                // while (!emitter.isRequested()) {
                // log.debug("emitter.isRequested: {}", emitter.isRequested());
                // Thread.sleep(10);
                // }
                if (json == null) {
                    // Thread.dumpStack();
                    log.warn("(json == null)");
                } else {
                    log.debug("<1> send json: {}", json);
                    // emitter.send(json.toString());
                }
                // if (true)
                // throw new RuntimeException("processNext error");
                return CompletableFuture.runAsync(() -> msg.ack());
            } catch (Throwable e) {
                log.error("", e);
                throw new RuntimeException(e);
            }
        }

        // ====
        @Incoming("addresses.transform")
        @Outgoing("addresses-next")
        // @Broadcast
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<ObjectNode> processAddresses(DebeziumMessage msg) {
            log.debug("msg.getTopic: {}", msg.getTopic());
            log.debug("msg.getKey: {}", msg.getKey());
            log.debug("msg.getPayload: {}", msg.getPayload());
            log.debug("msg.getHeaders: {}", msg.getHeaders());
            log.debug("msg.getTimestamp: {}",
                    LocalDateTime.ofInstant(new Date(msg.getTimestamp()).toInstant(), ZoneId.systemDefault()));

            ObjectNodeHelper helper = new ObjectNodeHelper();
            ObjectNode rootNode = helper.createObjectNode();
            rootNode.put("topic", msg.getTopic());

            return CompletableFuture //
                    .supplyAsync(() -> {
                        log.debug("supplyAsync, rootNode: {}", rootNode);
                        // if (true)
                        // throw new RuntimeException("supplyAsync error");
                        return rootNode;
                    }) //
                    .whenComplete((node, err) -> {
                        if (err != null) {
                            log.error("thenCompose, error", err);
                        } else {
                            log.debug("thenCompose, node: {}", node);
                        }
                    });
        }

        /**
         * 
         */
        @Incoming("addresses-next")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Void> processAddressesNext(ObjectNode json) {
            try {
                Runnable acion = () -> {
                    if (json == null) {
                        log.warn("(json == null)");
                    } else {
                        log.debug("<1> send json: {}", json);
                    }
                    //					if (true)
                    //						throw new RuntimeException("processNext error");
                };

                return CompletableFuture //
                        .runAsync(acion) //
                        .exceptionally(err -> {
                            log.error("error", err);
                            return null;
                        });
            } catch (Throwable e) {
                log.error("", e);
                throw new RuntimeException(e);
            }
        }
    }

}
