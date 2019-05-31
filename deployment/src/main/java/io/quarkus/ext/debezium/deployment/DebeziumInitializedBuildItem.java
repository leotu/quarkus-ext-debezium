package io.quarkus.ext.debezium.deployment;

import io.quarkus.builder.item.SimpleBuildItem;

/**
 * Marker build item indicating the Debezium has been fully initialized.
 */
public final class DebeziumInitializedBuildItem extends SimpleBuildItem {

    public DebeziumInitializedBuildItem() {
    }
}
