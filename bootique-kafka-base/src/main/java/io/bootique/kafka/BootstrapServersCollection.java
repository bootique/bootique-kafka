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


import java.util.Collection;
import java.util.Map;

/**
 * @since 1.0.RC1
 */
public class BootstrapServersCollection {

    private Map<String, BootstrapServers> clusters;

    public BootstrapServersCollection(Map<String, BootstrapServers> clusters) {
        if (clusters == null || clusters.isEmpty()) {
            throw new IllegalStateException("Kafka clusters are not configured");
        }

        this.clusters = clusters;
    }

    public BootstrapServers getDefaultCluster() {
        return getCluster(getDefaultName());
    }

    public BootstrapServers getCluster(String name) {
        BootstrapServers cluster = clusters.get(name);
        if (cluster == null) {
            throw new IllegalArgumentException("No configuration found for Kafka cluster name: '" + name + "'.");
        }

        return cluster;
    }

    private String getDefaultName() {

        Collection<String> allNames = clusters.keySet();

        switch (allNames.size()) {
            case 1:
                return allNames.iterator().next();
            default:
                throw new IllegalStateException("More then one cluster is available in configuration. " +
                        "Can't determine which one is the default. Configured cluser names: " + allNames);
        }
    }
}
