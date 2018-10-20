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

import io.bootique.kafka.BootstrapServersCollection;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

/**
 * @since 1.0.RC1
 */
public class DefaultKafkaProducerFactory implements KafkaProducerFactory {

    private BootstrapServersCollection clusters;
    private Map<String, String> properties;

    public DefaultKafkaProducerFactory(BootstrapServersCollection clusters, Map<String, String> properties) {
        this.clusters = Objects.requireNonNull(clusters);
        this.properties = Objects.requireNonNull(properties);
    }

    @Override
    public <K, V> DefaultKafkaProducerBuilder<K, V> producer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new DefaultKafkaProducerBuilder<>(clusters, properties, keySerializer, valueSerializer);
    }
}
