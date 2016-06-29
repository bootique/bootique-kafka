package io.bootique.kafka.client_0_8.consumer;


import kafka.javaapi.consumer.ConsumerConnector;

/**
 * An injectable factory for Kafka {@link ConsumerConnector} objects.
 */
public interface KafkaConsumerFactory {

    ConsumerConnector newConsumerConnector();

    ConsumerConnector newConsumerConnector(String name);

    ConsumerConnector newConsumerConnector(String name, ConsumerConfig configOverrides);
}
