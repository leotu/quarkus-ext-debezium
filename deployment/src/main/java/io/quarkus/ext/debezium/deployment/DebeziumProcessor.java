package io.quarkus.ext.debezium.deployment;

import org.jboss.logging.Logger;

import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem.BeanClassNameExclusion;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.substrate.ReflectiveClassBuildItem;
import io.quarkus.deployment.recording.RecorderContext;
import io.quarkus.ext.debezium.runtime.DebeziumKeyDeserializer;
import io.quarkus.ext.debezium.runtime.DebeziumValueDeserializer;

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
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DebeziumProcessor.class);

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

    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep
    void build(RecorderContext recorder, BuildProducer<ReflectiveClassBuildItem> reflectiveClass,
            BuildProducer<UnremovableBeanBuildItem> unremovableBeans) {

        unremovableBeans.produce(
                new UnremovableBeanBuildItem(new BeanClassNameExclusion(DebeziumKeyDeserializer.class.getName())));
        unremovableBeans.produce(
                new UnremovableBeanBuildItem(new BeanClassNameExclusion(DebeziumValueDeserializer.class.getName())));
    }

    @Record(ExecutionTime.RUNTIME_INIT)
    @BuildStep
    void configure(BuildProducer<DebeziumInitializedBuildItem> debeziumInitialized) {
        debeziumInitialized.produce(new DebeziumInitializedBuildItem());
    }

}
