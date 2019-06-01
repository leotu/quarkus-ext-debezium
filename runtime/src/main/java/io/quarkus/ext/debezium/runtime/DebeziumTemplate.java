package io.quarkus.ext.debezium.runtime;

import org.jboss.logging.Logger;

import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.arc.runtime.BeanContainerListener;
import io.quarkus.runtime.annotations.Template;

/**
 * Quarkus Template class (runtime)
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@Template
public class DebeziumTemplate {
    private static final Logger log = Logger.getLogger(DebeziumTemplate.class);

    /**
     * Build Time
     */
    public BeanContainerListener addContainerCreatedListener(
            Class<? extends AbstractDebeziumProducer> producerClassName, DebeziumConfig debeziumConfig) {

        return new BeanContainerListener() {

            /**
             * Runtime Time
             */
            @Override
            public void created(BeanContainer beanContainer) { // Arc.container()
                AbstractDebeziumProducer producer = beanContainer.instance(producerClassName);
                if (producer == null) {
                    log.warn("(producer == null)");
                } else {
                    log.debugv("producer.class: {0}", producer.getClass().getName());
                    producer.setDebeziumConfig(debeziumConfig);
                }
            }
        };
    }

}
