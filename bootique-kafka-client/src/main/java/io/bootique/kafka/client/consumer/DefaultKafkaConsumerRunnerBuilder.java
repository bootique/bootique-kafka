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

import io.bootique.kafka.BootstrapServersCollection;
import io.bootique.kafka.KafkaClientBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * @since 3.0.M1
 */
// TODO: is this useful at all? Should we deprecate the Runner?
public class DefaultKafkaConsumerRunnerBuilder<K, V>
        extends KafkaClientBuilder<KafkaConsumerRunnerBuilder<K, V>>
        implements KafkaConsumerRunnerBuilder<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaConsumerRunnerBuilder.class);

    private final KafkaConsumersManager consumersManager;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final Collection<String> topics;
    private Duration pollInterval;
    private String group;
    private Boolean autoCommit;
    private Duration autoCommitInterval;
    private AutoOffsetReset autoOffsetReset;
    private Duration sessionTimeout;

    public DefaultKafkaConsumerRunnerBuilder(
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
    public KafkaConsumerRunnerBuilder<K, V> topics(String... topics) {
        Collections.addAll(this.topics, topics);

        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> group(String group) {
        this.group = group;
        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> autoCommitInterval(Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> sessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    @Override
    public KafkaConsumerRunnerBuilder<K, V> pollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    @Override
    public KafkaConsumerRunner<K, V> create() {

        Properties properties = resolveProperties();

        // sanity check - KafkaConsumerRunner doesn't really work without auto-commit
        // see https://github.com/bootique/bootique-kafka/issues/30
        if (!"true".equals(properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
            throw new IllegalStateException("'KafkaConsumerRunner' can only be used when 'kafka.consumer.autoCommit' is 'true'");
        }

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
