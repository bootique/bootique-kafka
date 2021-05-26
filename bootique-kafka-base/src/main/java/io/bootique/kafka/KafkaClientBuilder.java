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
package io.bootique.kafka;

import org.apache.kafka.clients.CommonClientConfigs;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * A common superclass of Producer, Consumer and Stream builders that defines shared configuration patterns.
 */
public abstract class KafkaClientBuilder<T> {

    private BootstrapServersCollection clusters;
    private Map<String, String> defaultProperties;
    private Map<String, String> builderProperties;
    private String clusterName;

    protected KafkaClientBuilder(BootstrapServersCollection clusters, Map<String, String> defaultProperties) {
        this.clusters = Objects.requireNonNull(clusters);
        this.defaultProperties = Objects.requireNonNull(defaultProperties);
        this.builderProperties = new HashMap<>();
    }

    public T property(String key, String value) {
        this.builderProperties.put(key, value);
        return (T) this;
    }

    public T cluster(String clusterName) {
        this.clusterName = clusterName;
        return (T) this;
    }

    protected String resolveBootstrapServers() {
        BootstrapServers cluster = clusterName != null
                ? clusters.getCluster(clusterName)
                : clusters.getDefaultCluster();

        return cluster.asString();
    }

    protected Properties resolveProperties() {

        Properties combined = new Properties();

        // resolution order is significant... default (common, coming from YAML) -> builder generic -> builder explicit

        // 1. default
        combined.putAll(defaultProperties);

        // 2. builder generic
        combined.putAll(builderProperties);

        // *. allowing only one source for bootstrap server
        combined.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolveBootstrapServers());

        // 3. builder explicit
        appendBuilderProperties(combined);

        return combined;
    }

    protected abstract void appendBuilderProperties(Properties combined);
}
