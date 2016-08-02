package io.bootique.kafka.client;

import io.bootique.kafka.client.consumer.ConsumerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A configuration object that describes a a set of Kafka clusters  as well as a producer and consumer templates. Serves
 * as a factory for server producers and consumers.
 *
 * @since 0.2
 */
public class KafkaClientFactoryFactory {

    private Map<String, String> clusters;
    private ConsumerFactory consumer;

    public KafkaClientFactoryFactory() {
        this.consumer = new ConsumerFactory();
    }

    public void setConsumer(ConsumerFactory consumer) {
        this.consumer = consumer;
    }

    public Map<String, String> getClusters() {
        return clusters;
    }

    public void setClusters(Map<String, String> clusters) {
        this.clusters = clusters;
    }

    public DefaultKafkaClientFactory createFactory() {

        Map<String, String> clusters = this.clusters != null ? this.clusters : Collections.emptyMap();
        ConsumerFactory consumerTemplate = this.consumer != null ? this.consumer : new ConsumerFactory();
        return new DefaultKafkaClientFactory(clusters, consumerTemplate);
    }
}
