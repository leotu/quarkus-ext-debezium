package io.quarkus.ext.debezium.deployment;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.DotName;
import org.jboss.logging.Logger;

import io.quarkus.arc.deployment.BeanContainerListenerBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem.BeanClassNameExclusion;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.substrate.ReflectiveClassBuildItem;
import io.quarkus.deployment.recording.RecorderContext;
import io.quarkus.deployment.util.HashUtil;
import io.quarkus.ext.debezium.runtime.AbstractDebeziumProducer;
import io.quarkus.ext.debezium.runtime.AbstractDebeziumProducer.DebeziumQualifier;
import io.quarkus.ext.debezium.runtime.DebeziumConfig;
import io.quarkus.ext.debezium.runtime.DebeziumItemConfig;
import io.quarkus.ext.debezium.runtime.DebeziumKeyDeserializer;
import io.quarkus.ext.debezium.runtime.DebeziumMessage;
import io.quarkus.ext.debezium.runtime.DebeziumTemplate;
import io.quarkus.ext.debezium.runtime.DebeziumValueDeserializer;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

/**
 * Deployment Processor
 * 
 * <pre>
 * https://quarkus.io/guides/cdi-reference#supported_features
 * https://github.com/quarkusio/gizmo
 * https://github.com/debezium/debezium
 * </pre>
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
public class DebeziumProcessor {
    private static final Logger log = Logger.getLogger(DebeziumProcessor.class);

    public static final DotName INCOMING = DotName.createSimple(Incoming.class.getName());
    public static final DotName OUTGOING = DotName.createSimple(Outgoing.class.getName());
    public static final DotName BROADCAST = DotName.createSimple(Broadcast.class.getName());

    private static final DotName DEBEZIUM_QUALIFIER = DotName.createSimple(DebeziumQualifier.class.getName());

    private final String producerClassName = AbstractDebeziumProducer.class.getPackage().getName() + "."
            + "DebeziumProducer";

    /**
     * Register a extension capability and feature
     *
     * @return Debezium feature build item
     */
    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep(providesCapabilities = "io.quarkus.ext.debezium")
    FeatureBuildItem featureBuildItem() {
        return new FeatureBuildItem("debezium");
    }

    @SuppressWarnings("unchecked")
    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep
    BeanContainerListenerBuildItem build(RecorderContext recorder, DebeziumTemplate template,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClass, //
            BuildProducer<UnremovableBeanBuildItem> unremovableBeans, DebeziumConfig debeziumConfig,
            BuildProducer<GeneratedBeanBuildItem> generatedBean) {

        if (debeziumConfig.namedConfig.isEmpty()) {
            log.info("No incoming been defined");
            return null;
        }

        unremovableBeans.produce(
                new UnremovableBeanBuildItem(new BeanClassNameExclusion(DebeziumKeyDeserializer.class.getName())));
        unremovableBeans.produce(
                new UnremovableBeanBuildItem(new BeanClassNameExclusion(DebeziumValueDeserializer.class.getName())));

        createDebeziumProducerBean(generatedBean, unremovableBeans, debeziumConfig);

        return new BeanContainerListenerBuildItem(template.addContainerCreatedListener(
                (Class<? extends AbstractDebeziumProducer>) recorder.classProxy(producerClassName), debeziumConfig));
    }

    @Record(ExecutionTime.RUNTIME_INIT)
    @BuildStep
    void configure(DebeziumTemplate template, BuildProducer<DebeziumInitializedBuildItem> debeziumInitialized) {
        debeziumInitialized.produce(new DebeziumInitializedBuildItem());
    }

    public static Message<DebeziumMessage> of(DebeziumMessage data) {
        System.out.println("data.class: " + data.getClass().getName());
        return () -> data;
    }

    /**
     * incoming(KafkaMessage) , outgoing(DebeziumMessage)
     * </p>
     * <code>FIXME: @Acknowledgment(Acknowledgment.Strategy.MANUAL)</code>
     */
    private void createDebeziumProducerBean(BuildProducer<GeneratedBeanBuildItem> generatedBean,
            BuildProducer<UnremovableBeanBuildItem> unremovableBeans, DebeziumConfig debeziumConfig) {
        ClassOutput classOutput = new ClassOutput() {
            @Override
            public void write(String name, byte[] data) {
                generatedBean.produce(new GeneratedBeanBuildItem(name, data));
            }
        };
        unremovableBeans.produce(new UnremovableBeanBuildItem(new BeanClassNameExclusion(producerClassName)));

        ClassCreator classCreator = ClassCreator.builder().classOutput(classOutput).className(producerClassName)
                .superClass(AbstractDebeziumProducer.class).build();
        classCreator.addAnnotation(ApplicationScoped.class);

        debeziumConfig.namedConfig.forEach((named, namedConfig) -> {
            log.debugv("named: {0}, namedConfig: {1}", named, namedConfig);

            if (!isPresentOutgoing(namedConfig)) {
                log.warnv("!isPresentOutgoing(namedConfig), named: {0}, namedConfig: {1}", named, namedConfig);
            } else {
                String suffix = HashUtil.sha1(named);
                MethodCreator methodCreator = classCreator.getMethodCreator("transform_" + suffix,
                        DebeziumMessage.class, KafkaMessage.class);

                methodCreator.addAnnotation(AnnotationInstance.create(INCOMING, null,
                        new AnnotationValue[] { AnnotationValue.createStringValue("value", named) }));

                methodCreator.addAnnotation(AnnotationInstance.create(DEBEZIUM_QUALIFIER, null,
                        new AnnotationValue[] { AnnotationValue.createStringValue("value", named) }));

                methodCreator.addAnnotation(AnnotationInstance.create(OUTGOING, null,
                        new AnnotationValue[] { AnnotationValue.createStringValue("value", namedConfig.outgoing) }));

                namedConfig.broadcast.ifPresent(broadcast -> {
                    if (broadcast) {
                        methodCreator.addAnnotation(AnnotationInstance.create(BROADCAST, null, new AnnotationValue[] {
                                AnnotationValue.createIntegerValue("value", namedConfig.broadcastValue.getAsInt()) }));
                    }
                });

                ResultHandle arg0RH = methodCreator.getMethodParam(0);

                methodCreator.returnValue(methodCreator.invokeVirtualMethod( //
                        MethodDescriptor.ofMethod(AbstractDebeziumProducer.class, "transform", DebeziumMessage.class,
                                KafkaMessage.class),
                        methodCreator.getThis(), arg0RH));
            }
        });

        classCreator.close();
    }

    private boolean isPresentOutgoing(DebeziumItemConfig itemConfig) {
        return itemConfig.outgoing != null && !itemConfig.outgoing.isEmpty();
    }
}
