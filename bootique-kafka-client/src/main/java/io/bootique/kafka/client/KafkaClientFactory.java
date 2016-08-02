package io.bootique.kafka.client;

import io.bootique.kafka.client.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * An injectable service that helps to create Kafka producers and consumers based on Bootique configuration and
 * user-provided settings.
 *
 * @since 0.2
 */
public interface KafkaClientFactory {

    <K, V> Consumer<K, V> createConsumer(ConsumerConfig<K, V> config);

    <K, V> Consumer<K, V> createConsumer(String clusterName, ConsumerConfig<K, V> config);
}
