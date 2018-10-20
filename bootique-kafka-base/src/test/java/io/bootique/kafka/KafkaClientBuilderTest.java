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
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaClientBuilderTest {

    @Test
    public void testResolveProperties_Clusters() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c1", new BootstrapServers("example.org:5678"));
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        TestClientBuilder builder = new TestClientBuilder(
                clusters,
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        builder.cluster("c1");
        Properties resolved = builder.resolveProperties();
        assertEquals("example.org:5678", resolved.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testResolveProperties_Clusters_Default() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        TestClientBuilder builder = new TestClientBuilder(
                clusters,
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        Properties resolved = builder.resolveProperties();
        assertEquals("example.org:5679", resolved.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test(expected = IllegalStateException.class)
    public void testResolveProperties_Clusters_NoDefault() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c1", new BootstrapServers("example.org:5678"));
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        TestClientBuilder builder = new TestClientBuilder(
                clusters,
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        builder.resolveProperties();
    }

    @Test
    public void testResolveProperties_ExplicitVals() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Map<String, String> defaultProps = new HashMap<>();
        defaultProps.put("a", "d_a");
        defaultProps.put("b", "d_b");
        defaultProps.put("c", "d_c");

        Map<String, String> phase3Props = new HashMap<>();
        phase3Props.put("a", "3_a");
        phase3Props.put("b", "3_b");
        phase3Props.put("c", "3_c");

        TestClientBuilder builder = new TestClientBuilder(
                new BootstrapServersCollection(clustersMap),
                defaultProps,
                phase3Props
        );

        builder.property("a", "2_a")
                .property("b", "2_b")
                .property("c", "2_c");

        Properties resolved = builder.resolveProperties();
        assertEquals("3_a", resolved.getProperty("a"));
        assertEquals("3_b", resolved.getProperty("b"));
        assertEquals("3_c", resolved.getProperty("c"));
    }

    @Test
    public void testResolveProperties_CustomProperties() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Map<String, String> defaultProps = new HashMap<>();
        defaultProps.put("a", "d_a");
        defaultProps.put("b", "d_b");
        defaultProps.put("c", "d_c");

        TestClientBuilder builder = new TestClientBuilder(
                new BootstrapServersCollection(clustersMap),
                defaultProps,
                Collections.emptyMap()
        );

        builder.property("a", "2_a")
                .property("b", "2_b")
                .property("c", "2_c");

        Properties resolved = builder.resolveProperties();
        assertEquals("2_a", resolved.getProperty("a"));
        assertEquals("2_b", resolved.getProperty("b"));
        assertEquals("2_c", resolved.getProperty("c"));
    }

    @Test
    public void testResolveProperties_Defaults() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Map<String, String> defaultProps = new HashMap<>();
        defaultProps.put("a", "d_a");
        defaultProps.put("b", "d_b");
        defaultProps.put("c", "d_c");

        TestClientBuilder builder = new TestClientBuilder(
                new BootstrapServersCollection(clustersMap),
                defaultProps,
                Collections.emptyMap()
        );

        Properties resolved = builder.resolveProperties();
        assertEquals("d_a", resolved.getProperty("a"));
        assertEquals("d_b", resolved.getProperty("b"));
        assertEquals("d_c", resolved.getProperty("c"));
    }

    private static class TestClientBuilder extends KafkaClientBuilder<TestClientBuilder> {
        private Map<String, String> phase3Properties;

        public TestClientBuilder(BootstrapServersCollection clusters,
                                 Map<String, String> defaultProperties,
                                 Map<String, String> phase3Properties) {
            super(clusters, defaultProperties);
            this.phase3Properties = phase3Properties;
        }

        @Override
        protected void appendBuilderProperties(Properties combined) {
            combined.putAll(phase3Properties);
        }
    }
}
