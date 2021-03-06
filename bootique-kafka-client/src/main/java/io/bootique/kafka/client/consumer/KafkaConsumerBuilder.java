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

import java.time.Duration;


public interface KafkaConsumerBuilder<K, V> {

    KafkaConsumerRunner<K, V> create();

    /**
     * Sets a custom property for the underlying Consumer object being built. This property will override any defaults,
     * specified via Bootique config.
     *
     * @param key
     * @param value
     * @return this builder instance
     */
    KafkaConsumerBuilder<K, V> property(String key, String value);

    /**
     * Sets a symbolic Kafka cluster name to use. The cluster under this name should have been configured in the
     * the Bootique app. If not set, a default cluster will be located in the config.
     *
     * @param clusterName symbolic name of the cluster that must reference a known cluster in config.
     * @return this builder instance
     */
    KafkaConsumerBuilder<K, V> cluster(String clusterName);

    KafkaConsumerBuilder<K, V> topics(String... topics);

    KafkaConsumerBuilder<K, V> group(String group);

    KafkaConsumerBuilder<K, V> autoCommitInterval(Duration duration);

    KafkaConsumerBuilder<K, V> autoCommit(boolean autoCommit);

    KafkaConsumerBuilder<K, V> autoOffsetReset(AutoOffsetReset autoOffsetReset);

    KafkaConsumerBuilder<K, V> sessionTimeout(Duration duration);

    KafkaConsumerBuilder<K, V> pollInterval(Duration pollInterval);
}
