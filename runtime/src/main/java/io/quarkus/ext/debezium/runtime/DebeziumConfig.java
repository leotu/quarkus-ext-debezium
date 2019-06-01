package io.quarkus.ext.debezium.runtime;

import java.util.Map;

import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 * Read from application.properties file
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 *
 */
@ConfigRoot(name = "debezium.incoming", phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public class DebeziumConfig {

    /**
     * The incoming config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public Map<String, DebeziumItemConfig> namedConfig;

}
