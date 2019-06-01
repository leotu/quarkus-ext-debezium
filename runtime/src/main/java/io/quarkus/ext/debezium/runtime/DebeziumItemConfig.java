package io.quarkus.ext.debezium.runtime;

import java.util.Optional;
import java.util.OptionalInt;

import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@ConfigGroup
public class DebeziumItemConfig {

    /**
     * The outgoing name
     */
    @ConfigItem
    public String outgoing;

    /**
     * The broadcast
     * 
     * @see io.smallrye.reactive.messaging.annotations.Broadcast
     */
    @ConfigItem(defaultValue = "false")
    public Optional<Boolean> broadcast;

    /**
     * The broadcast value
     * 
     * @see io.smallrye.reactive.messaging.annotations.Broadcast#value()
     */
    @ConfigItem(defaultValue = "1")
    public OptionalInt broadcastValue;

    @Override
    public String toString() {
        return super.toString() + "{outgoing:" + outgoing + ", broadcast:" + broadcast + ", broadcastValue:"
                + broadcastValue + "}";
    }

}
