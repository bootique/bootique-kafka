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

import io.bootique.kafka.KafkaClientBuilder;
import io.bootique.kafka.BootstrapServersCollection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @since 1.0.RC1
 */
public class DefaultKafkaConsumerBuilder<K, V> extends KafkaClientBuilder<KafkaConsumerBuilder<K, V>> implements KafkaConsumerBuilder<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaConsumerBuilder.class);

    private KafkaConsumersManager consumersManager;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;

    private Collection<String> topics;
    private Duration pollInterval;
    private String group;
    private Boolean autoCommit;
    private Duration autoCommitInterval;
    private AutoOffsetReset autoOffsetReset;
    private Duration sessionTimeout;

    public DefaultKafkaConsumerBuilder(
            KafkaConsumersManager consumersManager,
            BootstrapServersCollection clusters,
            Map<String, String> defaultProperties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        super(clusters, defaultProperties);

        this.consumersManager = Objects.requireNonNull(consumersManager);
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer);
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer);

        this.topics = new ArrayList<>(3);
    }

    @Override
    public KafkaConsumerBuilder<K, V> topics(String... topics) {
        for (String t : topics) {
            this.topics.add(t);
        }

        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> group(String group) {
        this.group = group;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> autoCommitInterval(Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> sessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> pollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    @Override
    public KafkaConsumerRunner<K, V> create() {

        Properties properties = resolveProperties();
        Collection<String> topics = createTopics();

        LOGGER.info("Creating consumer. Cluster: {}, topics: {}.",
                properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                topics);

        return new KafkaConsumerRunner<>(consumersManager, createConsumer(properties), createTopics(), createPollInterval());
    }

    protected Consumer<K, V> createConsumer(Properties properties) {
        return new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
    }

    protected Collection<String> createTopics() {

        if (topics.isEmpty()) {
            throw new IllegalStateException("No consumption topics configured");
        }

        return topics;
    }

    protected Duration createPollInterval() {
        return pollInterval != null ? pollInterval : Duration.ofMillis(100);
    }


    @Override
    protected void appendBuilderProperties(Properties combined) {
        if (group != null) {
            combined.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        }

        if (autoCommit != null) {
            combined.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        }

        if (autoCommitInterval != null) {
            combined.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval.toMillis()));
        }

        if (sessionTimeout != null) {
            combined.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeout.toMillis()));
        }

        if (autoOffsetReset != null) {
            combined.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.name());
        }
    }
}
