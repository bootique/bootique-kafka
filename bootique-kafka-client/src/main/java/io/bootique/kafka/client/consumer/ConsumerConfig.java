/*
 * Licensed to ObjectStyle LLC under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ObjectStyle LLC licenses
 * this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.bootique.kafka.client.consumer;

import io.bootique.kafka.BootstrapServers;
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
    private OffsetReset autoOffsetReset;
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

    /**
     * @return
     * @since 1.0.RC1
     */
    public OffsetReset getAutoOffsetReset() {
        return autoOffsetReset;
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

        public Builder<K, V> autoOffsetRest(OffsetReset autoOffsetReset) {
            config.autoOffsetReset = autoOffsetReset;
            return this;
        }

        public Builder<K, V> sessionTimeoutMs(int ms) {
            config.sessionTimeoutMs = ms;
            return this;
        }

        /**
         * Sets a comma-separated list of Kafka bootstrap servers for the consumer. Usually this method is not used,
         * as the servers are set in configuration under "kafkaclient.clusters" key.
         *
         * @param bootstrapServers a comma-separated list of Kafka bootstrap servers. E.g. "localhost:9092".
         * @return
         */
        public Builder<K, V> bootstrapServers(String bootstrapServers) {
            config.bootstrapServers = bootstrapServers != null ? new BootstrapServers(bootstrapServers) : null;
            return this;
        }

        /**
         * Sets a collection of Kafka bootstrap servers for the consumer. Usually this method is not used, as the
         * servers are set in configuration under "kafkaclient.clusters" key.
         *
         * @param bootstrapServers a collection of Kafka bootstrap servers with ports.
         * @return
         */
        public Builder<K, V> bootstrapServers(Collection<String> bootstrapServers) {
            config.bootstrapServers = bootstrapServers != null && !bootstrapServers.isEmpty()
                    ? BootstrapServers.create(bootstrapServers)
                    : null;
            return this;
        }
    }
}
