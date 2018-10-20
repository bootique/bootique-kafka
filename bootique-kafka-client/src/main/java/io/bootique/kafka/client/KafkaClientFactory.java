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

package io.bootique.kafka.client;

import io.bootique.kafka.client.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

/**
 * A low-level injectable service that helps to create Kafka producers and consumers based on Bootique configuration and
 * user-provided settings. When working with consumers, consider injecting {@link io.bootique.kafka.client.consumer.KafkaConsumerFactory}
 * instead.
 *
 * @see io.bootique.kafka.client.consumer.KafkaConsumerFactory
 * @since 0.2
 */
public interface KafkaClientFactory {
    
    <K, V> Producer<K, V> createProducer(ProducerConfig<K, V> config);

    <K, V> Producer<K, V> createProducer(String clusterName, ProducerConfig<K, V> config);
}
