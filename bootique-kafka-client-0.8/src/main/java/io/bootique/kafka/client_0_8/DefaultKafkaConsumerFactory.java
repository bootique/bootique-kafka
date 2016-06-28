package io.bootique.kafka.client_0_8;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Collection;
import java.util.Map;

public class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {

    private Map<String, ConsumerConfig> consumerConfigs;

    public DefaultKafkaConsumerFactory(Map<String, ConsumerConfig> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    @Override
    public ConsumerConnector newConsumerConnector() {

        Collection<String> allNames = consumerConfigs.keySet();

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

        ConsumerConfig config = consumerConfigs.get(name);

        if (config == null) {
            throw new IllegalArgumentException("Kafka consumer is not configured: " + name);
        }

        return Consumer.createJavaConsumerConnector(config);
    }
}
