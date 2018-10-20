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

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.BootstrapServers;
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
    private static final String AUTO_OFFSET_RESET_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
    private static final String SESSION_TIMEOUT_MS_CONFIG = org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

    private String defaultGroup;
    private boolean autoCommit;
    private AutoOffsetReset autoOffsetReset;
    private int autoCommitIntervalMs;
    private int sessionTimeoutMs;

    public ConsumerFactory() {
        this.autoOffsetReset = AutoOffsetReset.latest;
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

    /**
     * @param autoOffsetReset
     * @since 1.0.RC1
     */
    @BQConfigProperty
    public void setAutoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
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

        setProperty(properties,
                AUTO_OFFSET_RESET_CONFIG,
                config.getAutoOffsetReset(),
                autoOffsetReset);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Creating consumer bootstrapping with %s, group id: %s.",
                    properties.get(BOOTSTRAP_SERVERS_CONFIG),
                    properties.get(GROUP_ID_CONFIG)));
        }

        return new KafkaConsumer<>(properties, config.getKeyDeserializer(), config.getValueDeserializer());
    }
}
