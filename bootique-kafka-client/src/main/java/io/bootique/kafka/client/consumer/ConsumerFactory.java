package io.bootique.kafka.client.consumer;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.client.BootstrapServers;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.bootique.kafka.client.FactoryUtils.setProperty;
import static io.bootique.kafka.client.FactoryUtils.setRequiredProperty;

/**
 * A YAML-configurable factory for a base consumer configuration. In runtime Kafka consumer is created by merging this
 * configuration with user-provided {@link ConsumerConfig}.
 *
 * @since 0.2
 */
@BQConfig
public class ConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactory.class);

    private static final String BOOTSTRAP_SERVERS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String GROUP_ID_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
    private static final String ENABLE_AUTO_COMMIT_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
    private static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

    private String defaultGroup;
    private boolean autoCommit;
    private int autoCommitIntervalMs;
    private int sessionTimeoutMs;

    public ConsumerFactory() {
        this.autoCommit = true;
        this.autoCommitIntervalMs = 1000;
        this.sessionTimeoutMs = 30000;
    }

    @BQConfigProperty
    public void setDefaultGroup(String defaultGroup) {
        this.defaultGroup = defaultGroup;
    }

    @BQConfigProperty
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @BQConfigProperty
    public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    @BQConfigProperty
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
            LOGGER.info(String.format("Creating consumer bootstrapping with %s, group id: %s.",
                    properties.get(BOOTSTRAP_SERVERS_CONFIG),
                    properties.get(GROUP_ID_CONFIG)));
        }

        return new KafkaConsumer<>(properties, config.getKeyDeserializer(), config.getValueDeserializer());
    }


}
