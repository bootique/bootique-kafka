package io.bootique.kafka.client.consumer;

import org.apache.kafka.common.serialization.Deserializer;

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
    private Long autoCommitIntervalMs;
    private Long sessionTimeoutMs;

    public ConsumerConfig(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer);
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer);
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

    public Long getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public Long getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }
}
