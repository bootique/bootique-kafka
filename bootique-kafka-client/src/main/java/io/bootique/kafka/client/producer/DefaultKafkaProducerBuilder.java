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

package io.bootique.kafka.client.producer;

import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.BootstrapServersCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @since 0.2
 */
public class DefaultKafkaProducerBuilder<K, V> implements KafkaProducerBuilder<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducerBuilder.class);

    private BootstrapServersCollection clusters;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;

    private Map<String, String> defaultProperties;
    private Map<String, String> builderProperties;

    private String clusterName;
    private String acks;
    private Integer retries;
    private Integer batchSize;
    private Duration linger;
    private Integer bufferMemory;

    public DefaultKafkaProducerBuilder(
            BootstrapServersCollection clusters,
            Map<String, String> defaultProperties,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {

        this.clusters = Objects.requireNonNull(clusters);
        this.defaultProperties = defaultProperties;
        this.keySerializer = Objects.requireNonNull(keySerializer);
        this.valueSerializer = Objects.requireNonNull(valueSerializer);
        this.builderProperties = new HashMap<>();
    }

    @Override
    public KafkaProducerBuilder<K, V> property(String key, String value) {
        this.builderProperties.put(key, value);
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> cluster(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> allAcks() {
        this.acks = "all";
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> acks(int acks) {
        this.acks = String.valueOf(acks);
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> retries(int retries) {
        this.retries = retries;
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> linger(Duration linger) {
        this.linger = linger;
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> bufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
        return this;
    }

    @Override
    public Producer<K, V> create() {
        Properties properties = resolveProperties();
        LOGGER.info("Creating producer. Cluster: {}.", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        return new KafkaProducer<>(properties, keySerializer, valueSerializer);
    }

    protected Properties resolveProperties() {

        Properties combined = new Properties();

        // resolution order is significant... default (common, coming from YAML) -> per-stream generic -> explicit
        combined.putAll(defaultProperties);
        combined.putAll(builderProperties);

        combined.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, resolveBootstrapServers());

        if (acks != null) {
            combined.put(ProducerConfig.ACKS_CONFIG, acks);
        }

        if (retries != null) {
            combined.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(retries));
        }

        if (batchSize != null) {
            combined.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        }

        if (linger != null) {
            combined.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(linger.toMillis()));
        }

        if (bufferMemory != null) {
            combined.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferMemory));
        }

        return combined;
    }

    protected String resolveBootstrapServers() {
        BootstrapServers cluster = clusterName != null
                ? clusters.getCluster(clusterName)
                : clusters.getDefaultCluster();

        return cluster.asString();
    }
}
