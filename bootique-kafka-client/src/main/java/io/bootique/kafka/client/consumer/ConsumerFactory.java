package io.bootique.kafka.client.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A YAML-configurable factory for a base consumer configuration. In runtime Kafka consumer is created by merging this
 * configuration with user-provided {@link ConsumerConfig}.
 *
 * @since 0.2
 */
public class ConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactory.class);

    private static final String BOOTSTRAP_SERVERS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String GROUP_ID_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
    private static final String ENABLE_AUTO_COMMIT_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
    private static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

    private String defaultGroup;
    private boolean autoCommit;
    private long autoCommitIntervalMs;
    private int sessionTimeoutMs;

    public ConsumerFactory() {
        this.autoCommit = true;
        this.autoCommitIntervalMs = 1000l;
        this.sessionTimeoutMs = 30000;
    }

    public void setDefaultGroup(String defaultGroup) {
        this.defaultGroup = defaultGroup;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public <K, V> Consumer<K, V> createConsumer(BootstrapServers bootstrapServers, ConsumerConfig<K, V> config) {

        Map<String, Object> properties = new HashMap<>();

        setRequiredProperty(properties,
                BOOTSTRAP_SERVERS_CONFIG,
                Objects.requireNonNull(bootstrapServers).asString());

        setRequiredProperty(properties,
                GROUP_ID_CONFIG,
                config.getGroup(),
                defaultGroup);

        setProperty(properties,
                ENABLE_AUTO_COMMIT_CONFIG,
                config.getAutoCommit(),
                autoCommit);

        setProperty(properties,
                AUTO_COMMIT_INTERVAL_MS_CONFIG,
                config.getAutoCommitIntervalMs(),
                autoCommitIntervalMs);

        setProperty(properties,
                SESSION_TIMEOUT_MS_CONFIG,
                config.getSessionTimeoutMs(),
                sessionTimeoutMs);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Creating consumer bootstrapping to %s, group id: %s.",
                    properties.get(BOOTSTRAP_SERVERS_CONFIG),
                    properties.get(GROUP_ID_CONFIG)));
        }

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
