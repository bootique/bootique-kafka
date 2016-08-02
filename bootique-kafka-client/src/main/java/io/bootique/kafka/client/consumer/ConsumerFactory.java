package io.bootique.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * A YAML-configurable factory for a base consumer configuration. In runtime Kafka consumer is created by merging this
 * configuration with user-provided {@link ConsumerConfig}.
 *
 * @since 0.2
 */
public class ConsumerFactory {

    private String defaultGroup;
    private boolean autoCommit;
    private long autoCommitIntervalMs;
    private long sessionTimeoutMs;

    public void setDefaultGroup(String defaultGroup) {
        this.defaultGroup = defaultGroup;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public void setSessionTimeoutMs(long sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public <K, V> Consumer<K, V> createConsumer(String bootstrapServers, ConsumerConfig<K, V> config) {

        Map<String, Object> properties = new HashMap<>();

        setRequiredProperty(properties,
                org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        setRequiredProperty(properties,
                org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,
                config.getGroup(),
                defaultGroup);

        setProperty(properties,
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                config.getAutoCommit(),
                autoCommit);

        setProperty(properties,
                org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                config.getAutoCommitIntervalMs(),
                autoCommitIntervalMs);

        setProperty(properties,
                org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                config.getSessionTimeoutMs(),
                sessionTimeoutMs);

        return new KafkaConsumer<>(properties, config.getKeyDeserializer(), config.getValueDeserializer());
    }

    void setRequiredProperty(Map<String, Object> map, String key, Object... valueChoices) {
        Object v = firstNonNull(valueChoices);
        if (v == null) {
            throw new IllegalArgumentException("Missing required Kafka consumer property: " + key);
        }

        map.put(key, v);
    }

    void setProperty(Map<String, Object> map, String key, Object... valueChoices) {

        Object v = firstNonNull(valueChoices);
        if (v != null) {
            map.put(key, v);
        }
    }

    Object firstNonNull(Object... values) {
        if (values == null) {
            return null;
        }

        for (Object v : values) {
            if (v != null) {
                return v;
            }
        }

        return null;
    }
}
