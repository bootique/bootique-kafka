package io.bootique.kafka.client_0_8;


import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Map;

/**
 * An injectable factory for Kafka {@link ConsumerConnector} objects.
 */
public interface KafkaConsumerFactory {

    ConsumerConnector newConsumerConnector();

    ConsumerConnector newConsumerConnector(String name);

    ConsumerConnector newConsumerConnector(String name, Map<String, String> properties);
}
