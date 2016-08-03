package io.bootique.kafka.client;

import io.bootique.kafka.client.consumer.BootstrapServers;
import io.bootique.kafka.client.consumer.ConsumerConfig;
import io.bootique.kafka.client.consumer.ConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Collection;
import java.util.Map;

/**
 * @since 0.2
 */
public class DefaultKafkaClientFactory implements KafkaClientFactory {

    private Map<String, BootstrapServers> clusters;
    private ConsumerFactory consumerTemplate;

    public DefaultKafkaClientFactory(Map<String, BootstrapServers> clusters, ConsumerFactory consumerTemplate) {
        this.clusters = clusters;
        this.consumerTemplate = consumerTemplate;
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(ConsumerConfig<K, V> config) {
        return createConsumer(getDefaultName(), config);
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(String clusterName, ConsumerConfig<K, V> config) {
        BootstrapServers servers = config.getBootstrapServers();

        if (servers == null) {

            servers = clusters.get(clusterName);
            if (servers == null) {
                throw new IllegalArgumentException("Kafka bootstrap servers are missing for: " + clusterName);
            }
        }

        return consumerTemplate.createConsumer(servers, config);
    }

    private String getDefaultName() {

        if (clusters == null) {
            throw new IllegalStateException("Kafka clusters are not configured");
        }

        Collection<String> allNames = clusters.keySet();

        switch (allNames.size()) {
            case 0:
                throw new IllegalStateException("Kafka clusters are not configured");
            case 1:
                return allNames.iterator().next();
            default:
                throw new IllegalStateException("Default Kafka clusters name ambiguity. " +
                        "More then one cluster is provided in configuration.");
        }
    }
}
