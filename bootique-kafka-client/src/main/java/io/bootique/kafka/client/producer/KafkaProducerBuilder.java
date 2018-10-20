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

import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;

public interface KafkaProducerBuilder<K, V> {

    // TODO: wrap in a "runner" like we do for consumer and streams to handle shutdown
    Producer<K, V> create();

    KafkaProducerBuilder<K, V> property(String key, String value);

    KafkaProducerBuilder<K, V> cluster(String clusterName);

    KafkaProducerBuilder<K, V> allAcks();

    KafkaProducerBuilder<K, V> acks(int acks);

    KafkaProducerBuilder<K, V> retries(int retries);

    KafkaProducerBuilder<K, V> batchSize(int batchSize);

    KafkaProducerBuilder<K, V> linger(Duration linger);

    KafkaProducerBuilder<K, V> bufferMemory(int bufferMemory);
}
