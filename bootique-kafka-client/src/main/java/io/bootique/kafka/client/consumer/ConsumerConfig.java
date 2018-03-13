package io.bootique.kafka.client.consumer;

import io.bootique.kafka.client.BootstrapServers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Objects;

/**
 * A user-assembled configuration object that defines new consumer parameters. Some, such as deserializers are required.
 * Others are optional and override Bootique configuration defaults.
 *
 * @since 0.2
 */
public class ConsumerConfig<K, V> {

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private String group;
    private Boolean autoCommit;
    private Integer autoCommitIntervalMs;
    private Integer sessionTimeoutMs;
    private BootstrapServers bootstrapServers;

    private ConsumerConfig() {
    }

    public static Builder<byte[], byte[]> binaryConfig() {
        ByteArrayDeserializer d = new ByteArrayDeserializer();
        return new Builder(d, d);
    }

    public static <V> Builder<byte[], V> binaryKeyConfig(Deserializer<V> valueDeserializer) {
        ByteArrayDeserializer d = new ByteArrayDeserializer();
        return new Builder(d, valueDeserializer);
    }

    public static Builder<byte[], String> charValueConfig() {
        return new Builder(new ByteArrayDeserializer(), new StringDeserializer());
    }

    public static <K, V> Builder<K, V> config(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new Builder(keyDeserializer, valueDeserializer);
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public String getGroup() {
        return group;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public Integer getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public Integer getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public BootstrapServers getBootstrapServers() {
        return bootstrapServers;
    }

    public static class Builder<K, V> {
        private ConsumerConfig<K, V> config;

        public Builder(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
            this.config = new ConsumerConfig<>();
            this.config.keyDeserializer = Objects.requireNonNull(keyDeserializer);
            this.config.valueDeserializer = Objects.requireNonNull(valueDeserializer);
        }

        public ConsumerConfig<K, V> build() {
            return config;
        }

        public Builder<K, V> group(String group) {
            config.group = group;
            return this;
        }

        public Builder<K, V> autoCommitIntervalMs(int ms) {
            config.autoCommitIntervalMs = ms;
            return this;
        }

        public Builder<K, V> autoCommit(boolean autoCommit) {
            config.autoCommit = autoCommit;
            return this;
        }

        public Builder<K, V> sessionTimeoutMs(int ms) {
            config.sessionTimeoutMs = ms;
            return this;
        }

        public Builder<K, V> bootstrapServers(String bootstrapServers) {
            config.bootstrapServers = bootstrapServers != null ? new BootstrapServers(bootstrapServers) : null;
            return this;
        }

        public Builder<K, V> bootstrapServers(Collection<String> bootstrapServers) {
            config.bootstrapServers = bootstrapServers != null && !bootstrapServers.isEmpty()
                    ? new BootstrapServers(bootstrapServers)
                    : null;
            return this;
        }
    }
}
