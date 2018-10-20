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

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @since 1.0.RC1
 */
public interface KafkaProducerFactory {

    default DefaultKafkaProducerBuilder<byte[], String> charValueConfig() {
        return producer(new ByteArraySerializer(), new StringSerializer());
    }

    default DefaultKafkaProducerBuilder<byte[], byte[]> binaryProducer() {
        return producer(new ByteArraySerializer(), new ByteArraySerializer());
    }

    default <V> DefaultKafkaProducerBuilder<byte[], V> binaryKeyProoducer(Serializer<V> valueSerializer) {
        return producer(new ByteArraySerializer(), valueSerializer);
    }

    <K, V> DefaultKafkaProducerBuilder<K, V> producer(Serializer<K> keySerializer, Serializer<V> valueSerializer);
}
