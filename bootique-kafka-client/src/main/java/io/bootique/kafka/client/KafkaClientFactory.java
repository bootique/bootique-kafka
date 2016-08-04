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

    /**
     * Creates and returns a {@link Consumer} by merging default Bootique configuration with settings provided in a
     * {@link ConsumerConfig} argument. Uses default Kafka cluster connection information (default == single configuration
     * found in Bootique; if none or more than one exists, an exception is thrown). Returned Consumer needs to be
     * closed by the calling code when it is no longer in use.
     *
     * @param config Configuration of consumer specific to the given method call.
     * @param <K>    Consumed message key type.
     * @param <V>    Consumed message value type.
     * @return a new instance of Consumer.
     */
    <K, V> Consumer<K, V> createConsumer(ConsumerConfig<K, V> config);

    /**
     * Creates and returns a {@link Consumer} by merging default Bootique configuration with settings provided in a
     * {@link ConsumerConfig} argument. Returned Consumer needs to be closed by the calling code when it is no longer
     * in use.
     *
     * @param clusterName symbolic configuration name for the Kafka cluser coming from configuration.
     * @param config      Configuration of consumer specific to the given method call.
     * @param <K>         Consumed message key type.
     * @param <V>         Consumed message value type.
     * @return a new instance of Consumer.
     */
    <K, V> Consumer<K, V> createConsumer(String clusterName, ConsumerConfig<K, V> config);
}
