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
import io.bootique.kafka.client.KafkaResourceManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * @since 3.0
 */
public class KafkaConsumerBuilder<K, V> extends KafkaClientBuilder<KafkaConsumerBuilder<K, V>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerBuilder.class);

    private final KafkaResourceManager resourceManager;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Collection<String> topics;

    private String group;
    private Boolean autoCommit;
    private Duration autoCommitInterval;
    private AutoOffsetReset autoOffsetReset;
    private Duration sessionTimeout;
    private ConsumerRebalanceListener rebalanceListener;

    public KafkaConsumerBuilder(
            BootstrapServersCollection clusters,
            Map<String, String> defaultProperties,
            KafkaResourceManager resourceManager,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {

        super(clusters, defaultProperties);

        this.resourceManager = Objects.requireNonNull(resourceManager);
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer);
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer);

        this.topics = new ArrayList<>(3);
    }

    /**
     * Provides a quick way to consume Kafka messages. Internally creates a new consumer, subscribes it to the specified
     * topics, and starts Kafka polling, invoking the provided callback on each batch of data read. This method
     * is non-blocking, and returns immediately. To stop consumption, you may call {@link KafkaPollingTracker#close()} on the
     * returned "poller" object.
     */
    public KafkaPollingTracker consume(KafkaConsumerCallback<K, V> callback, Duration pollInterval) {
        Consumer<K, V> unmanaged = createUnmanagedConsumer();
        Consumer<K, V> subscribed = subscribe(unmanaged);
        return new KafkaPollingTracker(resourceManager, subscribed, callback, pollInterval);
    }

    /**
     * Creates a consumer, configures it using builder settings, and subscribes it to the builder topics. This method
     * should be used if {@link #consume(KafkaConsumerCallback, Duration)} is not sufficient, and the caller needs
     * more fine-grained control over the process of reading Kafka queue.
     */
    public Consumer<K, V> createConsumer() {
        Consumer<K, V> unmanaged = createUnmanagedConsumer();
        Consumer<K, V> managed = new ManagedConsumer<>(resourceManager, unmanaged);
        return subscribe(managed);
    }

    protected Consumer<K, V> subscribe(Consumer<K, V> consumer) {

        if (topics.isEmpty()) {
            throw new IllegalStateException("No consumption topics configured");
        }

        LOGGER.info("Subscribing consumer {} to topics: {}", System.identityHashCode(consumer), topics);
        ConsumerRebalanceListener rebalanceListener = this.rebalanceListener != null
                ? this.rebalanceListener
                : new NoOpRebalanceListener();

        consumer.subscribe(topics, rebalanceListener);
        return consumer;
    }

    protected Consumer<K, V> createUnmanagedConsumer() {
        Properties properties = resolveProperties();
        Consumer<K, V> consumer = new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer);
        LOGGER.info("Creating consumer {}. Cluster: {}",
                System.identityHashCode(consumer),
                properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

        return consumer;
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

    /**
     * Sets a custom property for the underlying Consumer object being built. This property will override any defaults,
     * specified via Bootique config.
     *
     * @return this builder instance
     */
    @Override
    public KafkaConsumerBuilder<K, V> property(String key, String value) {
        return super.property(key, value);
    }

    /**
     * Sets a symbolic Kafka cluster name to use. The cluster under this name should have been configured in the
     * the Bootique app. If not set, a default cluster will be located in the config.
     *
     * @param clusterName symbolic name of the cluster that must reference a known cluster in config.
     * @return this builder instance
     */
    @Override
    public KafkaConsumerBuilder<K, V> cluster(String clusterName) {
        return super.cluster(clusterName);
    }

    public KafkaConsumerBuilder<K, V> topics(String... topics) {
        Collections.addAll(this.topics, topics);
        return this;
    }

    public KafkaConsumerBuilder<K, V> group(String group) {
        this.group = group;
        return this;
    }

    public KafkaConsumerBuilder<K, V> autoCommitInterval(Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
        return this;
    }

    public KafkaConsumerBuilder<K, V> autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public KafkaConsumerBuilder<K, V> autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        return this;
    }

    public KafkaConsumerBuilder<K, V> sessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public KafkaConsumerBuilder<K, V> rebalanceListener(ConsumerRebalanceListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
        return this;
    }

    static class NoOpRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }
    }
}
