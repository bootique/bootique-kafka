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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DefaultKafkaStreamsBuilderTest {

    @Test
    public void testResolveProperties_Clusters() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c1", new BootstrapServers("example.org:5678"));
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                clusters,
                new Properties()
        );

        builder.cluster("c1");
        Properties resolved = builder.resolveProperties();
        assertEquals("example.org:5678", resolved.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testResolveProperties_Clusters_Default() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                clusters,
                new Properties()
        );

        Properties resolved = builder.resolveProperties();
        assertEquals("example.org:5679", resolved.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test(expected = IllegalStateException.class)
    public void testResolveProperties_Clusters_NoDefault() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c1", new BootstrapServers("example.org:5678"));
        clustersMap.put("c2", new BootstrapServers("example.org:5679"));

        BootstrapServersCollection clusters = new BootstrapServersCollection(clustersMap);

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                clusters,
                new Properties()
        );

        builder.resolveProperties();
    }

    @Test
    public void testResolveProperties_ExplicitVals() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Properties defaultProps = new Properties();
        defaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "dappid");
        defaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());
        defaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());

        Properties customProps = new Properties();
        defaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "cappid");
        defaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        defaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                new BootstrapServersCollection(clustersMap),
                defaultProps
        );

        builder.properties(customProps)
                .applicationId("eappid")
                .keySerde(Serdes.StringSerde.class)
                .valueSerde(Serdes.IntegerSerde.class);

        Properties resolved = builder.resolveProperties();
        assertEquals("eappid", resolved.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(Serdes.StringSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(Serdes.IntegerSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }

    @Test
    public void testResolveProperties_CustomProperties() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Properties defaultProps = new Properties();
        defaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "dappid");
        defaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());
        defaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());

        Properties customProps = new Properties();
        defaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "cappid");
        defaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        defaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                new BootstrapServersCollection(clustersMap),
                defaultProps
        );

        builder.properties(customProps);

        Properties resolved = builder.resolveProperties();
        assertEquals("cappid", resolved.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(Serdes.LongSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(Serdes.LongSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }

    @Test
    public void testResolveProperties_Defaults() {

        Map<String, BootstrapServers> clustersMap = new HashMap<>();
        clustersMap.put("c", new BootstrapServers("example.org:5679"));

        Properties defaultProps = new Properties();
        defaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "dappid");
        defaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());
        defaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.BytesSerde.class.getName());


        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                mock(KafkaStreamsManager.class),
                new BootstrapServersCollection(clustersMap),
                defaultProps
        );

        Properties resolved = builder.resolveProperties();
        assertEquals("dappid", resolved.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(Serdes.BytesSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(Serdes.BytesSerde.class.getName(), resolved.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }
}
