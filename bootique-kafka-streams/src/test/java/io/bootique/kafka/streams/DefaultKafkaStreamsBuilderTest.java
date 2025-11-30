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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultKafkaStreamsBuilderTest {

    @Test
    public void appendBuilderProperties_All() {

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                new KafkaStreamsManager(),
                new BootstrapServersCollection(Map.of("a", new BootstrapServers("-"))),
                Collections.emptyMap()
        );

        builder.applicationId("p1")
                .keySerde(Serdes.StringSerde.class)
                .valueSerde(Serdes.IntegerSerde.class);

        Properties properties = new Properties();
        builder.appendBuilderProperties(properties);
        assertEquals("p1", properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(Serdes.StringSerde.class.getName(), properties.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(Serdes.IntegerSerde.class.getName(), properties.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }

    @Test
    public void appendBuilderProperties_Partial() {

        DefaultKafkaStreamsBuilder builder = new DefaultKafkaStreamsBuilder(
                new KafkaStreamsManager(),
                new BootstrapServersCollection(Map.of("a", new BootstrapServers("-"))),
                Collections.emptyMap()
        );

        builder.valueSerde(Serdes.IntegerSerde.class);

        Properties properties = new Properties();
        builder.appendBuilderProperties(properties);
        assertNull(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        assertNull(properties.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(Serdes.IntegerSerde.class.getName(), properties.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }

}
