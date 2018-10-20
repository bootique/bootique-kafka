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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

/**
 * @since 1.0.RC1
 */
public class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {

    private KafkaConsumersManager consumersManager;
    private BootstrapServersCollection clusters;
    private Map<String, String> properties;

    public DefaultKafkaConsumerFactory(
            KafkaConsumersManager consumersManager,
            BootstrapServersCollection clusters,
            Map<String, String> properties) {

        this.consumersManager = consumersManager;
        this.clusters = clusters;
        this.properties = properties;
    }

    @Override
    public KafkaConsumerBuilder<byte[], String> charValueConsumer() {
        return consumer(new ByteArrayDeserializer(), new StringDeserializer());
    }

    @Override
    public KafkaConsumerBuilder<byte[], byte[]> binaryConsumer() {
        return consumer(new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public <V> KafkaConsumerBuilder<byte[], V> binaryKeyConsumer(Deserializer<V> valueDeserializer) {
        return consumer(new ByteArrayDeserializer(), valueDeserializer);
    }

    @Override
    public <K, V> KafkaConsumerBuilder<K, V> consumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new DefaultKafkaConsumerBuilder<>(
                consumersManager,
                clusters,
                properties,
                keyDeserializer,
                valueDeserializer);
    }
}
