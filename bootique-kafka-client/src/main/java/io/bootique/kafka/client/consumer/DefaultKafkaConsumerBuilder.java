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

import io.bootique.kafka.client.KafkaClientFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @since 1.0.RC1
 */
public class DefaultKafkaConsumerBuilder<K, V> implements KafkaConsumerBuilder<K, V> {

    private KafkaConsumersManager consumersManager;
    private KafkaClientFactory clientFactory;

    // TODO: ConsumerConfig is obsolete with the advent of KafkaConsumerFactory. Deprecate it, and change
    // to a simple map of properties, similar to the StreamsBuilder.

    private ConsumerConfig.Builder<K, V> config;

    private Collection<String> topics;
    private String cluster;

    public DefaultKafkaConsumerBuilder(
            KafkaConsumersManager consumersManager,
            KafkaClientFactory clientFactory,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        this.consumersManager = consumersManager;
        this.clientFactory = clientFactory;
        this.config = ConsumerConfig.config(keyDeserializer, valueDeserializer);
        this.topics = new ArrayList<>(3);
    }

    @Override
    public KafkaConsumerBuilder<K, V> cluster(String clusterName) {
        this.cluster = clusterName;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> property(String key, String value) {
        config.property(key, value);
        return this;
    }

    @Override
    public KafkaConsumerRunner<K, V> create() {
        return new KafkaConsumerRunner(consumersManager, createConsumer(), createTopics(), createDuration());
    }

    protected Consumer<K, V> createConsumer() {
        ConsumerConfig<K, V> config = createConfig();
        return cluster != null ? clientFactory.createConsumer(cluster, config) : clientFactory.createConsumer(config);
    }

    protected ConsumerConfig<K, V> createConfig() {

    }

    protected Collection<String> createTopics() {

        if (topics.isEmpty()) {
            throw new IllegalStateException("No consumption topics configured");
        }

        return topics;
    }

    protected Duration createDuration() {

    }
}
