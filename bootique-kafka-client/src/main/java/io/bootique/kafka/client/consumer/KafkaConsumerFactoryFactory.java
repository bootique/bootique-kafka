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
import io.bootique.kafka.BootstrapServersCollection;
import io.bootique.value.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A YAML-configurable factory for a base consumer configuration. Kafka Consumers are created by merging this
 * configuration with user-provided properties.
 */
@BQConfig
public class KafkaConsumerFactoryFactory {

    private String defaultGroup;
    private Boolean autoCommit;
    private AutoOffsetReset autoOffsetReset;
    private Duration autoCommitInterval;
    private Duration sessionTimeout;
    private Duration heartbeatInterval;

    @BQConfigProperty
    public void setDefaultGroup(String defaultGroup) {
        this.defaultGroup = defaultGroup;
    }

    @BQConfigProperty
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @BQConfigProperty
    public void setAutoCommitInterval(Duration autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    @BQConfigProperty
    public void setSessionTimeout(Duration sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @BQConfigProperty
    public void setAutoOffsetReset(AutoOffsetReset autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    @BQConfigProperty
    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public DefaultKafkaConsumerFactory createConsumer(
            KafkaConsumersManager consumersManager,
            BootstrapServersCollection clusters) {

        return new DefaultKafkaConsumerFactory(consumersManager, clusters, createDefaultProperties());
    }

    protected Map<String, String> createDefaultProperties() {
        Map<String, String> properties = new HashMap<>();

        // Note that below we are converting all values to Strings. Kafka would probably work if we preserve some of them
        // as ints/longs/etc, but let's honor an implied contract of the Properties class - its values must be Strings.

        if (defaultGroup != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroup);
        }

        boolean autoCommit = this.autoCommit != null ? this.autoCommit : true;
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

        long autoCommitIntervalMs = autoCommitInterval != null ? autoCommitInterval.getDuration().toMillis() : 1000L;
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        long sessionTimeoutMs = sessionTimeout != null ? sessionTimeout.getDuration().toMillis() : 30000L;
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));

        AutoOffsetReset autoOffsetReset = this.autoOffsetReset != null ? this.autoOffsetReset : AutoOffsetReset.latest;
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.name());

        long heartbeatIntervalMs = heartbeatInterval != null ? heartbeatInterval.getDuration().toMillis() : 3000L;
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMs));

        return properties;
    }


}
