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

import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.BootstrapServersCollection;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @since 1.0.RC1
 */
public class DefaultKafkaStreamsBuilder implements KafkaStreamsBuilder {

    private KafkaStreamsManager streamsManager;
    private BootstrapServersCollection clusters;

    private Topology topology;
    private Properties defaultProperties;
    private Map<Object, Object> perStreamProperties;
    private String clusterName;
    private String applicationId;
    private Class<? extends Serde<?>> keySerde;
    private Class<? extends Serde<?>> valueSerde;

    public DefaultKafkaStreamsBuilder(
            KafkaStreamsManager streamsManager,
            BootstrapServersCollection clusters,
            Properties defaultProperties) {

        this.streamsManager = Objects.requireNonNull(streamsManager);
        this.clusters = Objects.requireNonNull(clusters);
        this.defaultProperties = Objects.requireNonNull(defaultProperties);
        this.perStreamProperties = new HashMap<>();
    }

    @Override
    public KafkaStreamsBuilder topology(Topology topology) {
        this.topology = topology;
        return this;
    }

    @Override
    public KafkaStreamsBuilder properties(Properties properties) {
        this.perStreamProperties.clear();
        this.perStreamProperties.putAll(properties);
        return this;
    }

    @Override
    public KafkaStreamsBuilder property(String key, String value) {
        this.perStreamProperties.put(key, value);
        return this;
    }

    @Override
    public KafkaStreamsBuilder cluster(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    @Override
    public KafkaStreamsBuilder keySerde(Class<? extends Serde<?>> serializerDeserializer) {
        this.keySerde = serializerDeserializer;
        return this;
    }

    @Override
    public KafkaStreamsBuilder valueSerde(Class<? extends Serde<?>> serializerDeserializer) {
        this.valueSerde = serializerDeserializer;
        return this;
    }

    @Override
    public KafkaStreamsBuilder applicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    @Override
    public KafkaStreamsRunner create() {
        return new KafkaStreamsRunner(streamsManager, createStreams());
    }

    protected KafkaStreams createStreams() {
        Objects.requireNonNull(topology, "KafkaStreams 'topology' is not set");
        return new KafkaStreams(topology, resolveProperties());
    }

    protected Properties resolveProperties() {

        Properties combined = new Properties(defaultProperties);

        if (perStreamProperties != null) {
            combined.putAll(perStreamProperties);
        }

        combined.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, resolveBootstrapServers());

        if (applicationId != null) {
            combined.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        }

        if (keySerde != null) {
            combined.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getName());
        }

        if (valueSerde != null) {
            combined.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getName());
        }

        return combined;
    }

    protected String resolveBootstrapServers() {
        BootstrapServers cluster = clusterName != null
                ? clusters.getCluster(clusterName)
                : clusters.getDefaultCluster();

        return cluster.asString();
    }
}
