package io.bootique.kafka.client_0_8;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {

    private Map<String, Map<String, String>> configs;

    public DefaultKafkaConsumerFactory(Map<String, Map<String, String>> configs) {
        this.configs = configs;
    }

    @Override
    public ConsumerConnector newConsumerConnector() {

        Collection<String> allNames = configs.keySet();

        switch (allNames.size()) {
            case 0:
                throw new IllegalStateException("Kafka consumers are not configured");
            case 1:
                return newConsumerConnector(allNames.iterator().next());
            default:
                throw new IllegalStateException("Default Kafka consumer name ambiguity. " +
                        "More then one consumer is provided in configuration.");
        }
    }

    @Override
    public ConsumerConnector newConsumerConnector(String name) {
        return newConsumerConnector(name, Collections.emptyMap());
    }

    @Override
    public ConsumerConnector newConsumerConnector(String name, Map<String, String> properties) {

        Properties mergedProps = new Properties();

        Map<String, String> config = configs.get(name);

        if (config != null) {
            mergedProps.putAll(config);
        }

        if (properties != null) {
            mergedProps.putAll(properties);
        }

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(mergedProps));
    }
}
