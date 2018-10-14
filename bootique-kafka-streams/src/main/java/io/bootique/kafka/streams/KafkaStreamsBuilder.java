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
package io.bootique.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;

/**
 * A builder of a custom {@link org.apache.kafka.streams.KafkaStreams} instance. Returned instance is wrapped in a
 * {@link KafkaStreamsRunner} to manage its startup and shutdown. The order of configuration loading (configs lower on
 * the list override those higher on the list):
 *
 * <ul>
 * <li>Configs from YAML. Those are the default properties for all streams that can be overridden during each
 * instance creation.</li>
 * <li>Configs passed to the {@link #property(String, String)} method</li>
 * <li>Configs set via "named" builder methods like {@link #applicationId(String)}, etc.</li>
 * </ul>
 *
 * @since 1.0.RC1
 */
public interface KafkaStreamsBuilder {

    KafkaStreamsBuilder topology(Topology topology);

    /**
     * Sets a custom property for the KafkaStreams object being built. This property will override any defaults,
     * specified via Bootique config.
     *
     * @param key
     * @param value
     * @return this builder instance
     */
    KafkaStreamsBuilder property(String key, String value);

    /**
     * Sets a symbolic Kafka cluster name to use. The cluster under this name should have been configured in the
     * the Bootique app. If not set, a default cluster will be located in the config.
     *
     * @param clusterName symbolic name of the cluster that must reference a known cluster in config.
     * @return this builder instance
     */
    KafkaStreamsBuilder cluster(String clusterName);

    KafkaStreamsBuilder applicationId(String applicationId);

    KafkaStreamsBuilder keySerde(Class<? extends Serde<?>> serializerDeserializer);

    KafkaStreamsBuilder valueSerde(Class<? extends Serde<?>> serializerDeserializer);

    KafkaStreamsRunner create();

    default KafkaStreamsRunner run() {
        KafkaStreamsRunner runner = create();
        runner.start();
        return runner;
    }
}
