/**
 *    Licensed to the ObjectStyle LLC under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ObjectStyle LLC licenses
 *  this file to you under the Apache License, Version 2.0 (the
 *  “License”); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.bootique.kafka.client.producer;

import io.bootique.kafka.client.BootstrapServers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Objects;

/**
 * @since 0.2
 */
public class ProducerConfig<K, V> {

    private BootstrapServers bootstrapServers;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;

    private String acks;
    private Integer retries;
    private Integer batchSize;
    private Integer lingerMs;
    private Integer bufferMemory;

    private ProducerConfig() {
    }

    public static Builder<byte[], byte[]> binaryConfig() {
        ByteArraySerializer d = new ByteArraySerializer();
        return new Builder(d, d);
    }

    public static <V> Builder<byte[], byte[]> binaryKeyConfig(Serializer<V> valueSerializer) {
        ByteArraySerializer d = new ByteArraySerializer();
        return new Builder(d, valueSerializer);
    }

    public static Builder<byte[], String> charValueConfig() {
        return new Builder(new ByteArraySerializer(), new StringSerializer());
    }

    public static <K, V> Builder<K, V> config(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new Builder(keySerializer, valueSerializer);
    }

    public BootstrapServers getBootstrapServers() {
        return bootstrapServers;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    public String getAcks() {
        return acks;
    }

    public Integer getRetries() {
        return retries;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getLingerMs() {
        return lingerMs;
    }

    public Integer getBufferMemory() {
        return bufferMemory;
    }

    public static class Builder<K, V> {
        private ProducerConfig<K, V> config;

        public Builder(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.config = new ProducerConfig<>();
            this.config.keySerializer = Objects.requireNonNull(keySerializer);
            this.config.valueSerializer = Objects.requireNonNull(valueSerializer);
        }

        public ProducerConfig<K, V> build() {
            return config;
        }

        public Builder<K, V> allAcks() {
            config.acks = "all";
            return this;
        }

        public Builder<K, V> acks(int acks) {
            config.acks = String.valueOf(acks);
            return this;
        }

        public Builder<K, V> retries(int retries) {
            config.retries = retries;
            return this;
        }

        public Builder<K, V> batchSize(int batchSize) {
            config.batchSize = batchSize;
            return this;
        }

        public Builder<K, V> lingerMs(int ms) {
            config.lingerMs = ms;
            return this;
        }

        public Builder<K, V> bufferMemory(int bufferMemory) {
            config.bufferMemory = bufferMemory;
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
