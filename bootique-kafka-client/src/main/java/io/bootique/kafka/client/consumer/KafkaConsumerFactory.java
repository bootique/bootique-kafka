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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


public interface KafkaConsumerFactory {

    default KafkaConsumerBuilder<byte[], byte[]> binaryConsumer() {
        return consumer(new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    default KafkaConsumerBuilder<byte[], String> charValueConsumer() {
        return consumer(new ByteArrayDeserializer(), new StringDeserializer());
    }

    default <V> KafkaConsumerBuilder<byte[], V> binaryKeyConsumer(Deserializer<V> valueDeserializer) {
        return consumer(new ByteArrayDeserializer(), valueDeserializer);
    }

    <K, V> KafkaConsumerBuilder<K, V> consumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer);

}
