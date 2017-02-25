package io.bootique.kafka.client;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.client.consumer.ConsumerFactory;
import io.bootique.kafka.client.producer.ProducerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A configuration object that describes a a set of Kafka servers  as well as a producer and consumer templates. Serves
 * as a factory for server producers and consumers.
 *
 * @since 0.2
 */
@BQConfig
public class KafkaClientFactoryFactory {

    private Map<String, BootstrapServers> clusters;
    private ConsumerFactory consumer;
    private ProducerFactory producer;

    public KafkaClientFactoryFactory() {
        this.consumer = new ConsumerFactory();
    }

    @BQConfigProperty
    public void setConsumer(ConsumerFactory consumer) {
        this.consumer = consumer;
    }

    @BQConfigProperty
    public void setProducer(ProducerFactory producer) {
        this.producer = producer;
    }

    public Map<String, BootstrapServers> getClusters() {
        return clusters;
    }

    @BQConfigProperty
    public void setClusters(Map<String, BootstrapServers> clusters) {
        this.clusters = clusters;
    }

    public DefaultKafkaClientFactory createFactory() {

        Map<String, BootstrapServers> clusters = this.clusters != null ? this.clusters : Collections.emptyMap();
        ConsumerFactory consumerTemplate = this.consumer != null ? this.consumer : new ConsumerFactory();
        ProducerFactory producerTemplate = this.producer != null ? this.producer : new ProducerFactory();

        return new DefaultKafkaClientFactory(clusters, consumerTemplate, producerTemplate);
    }
}
