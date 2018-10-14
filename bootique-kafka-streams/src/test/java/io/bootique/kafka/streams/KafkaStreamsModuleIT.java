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

import io.bootique.BQRuntime;
import io.bootique.test.junit.BQTestFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class KafkaStreamsModuleIT {

    @Rule
    public final BQTestFactory testFactory = new BQTestFactory();

    @Test
    public void testKafkaStreamsFactory() {
        BQRuntime runtime = testFactory
                .app("-c", "classpath:io/bootique/kafka/streams/KafkaStreamsModuleIT.yml")
                .autoLoadModules()
                .createRuntime();

        KafkaStreamsFactory factory = runtime.getInstance(KafkaStreamsFactory.class);
        assertNotNull(factory);
    }

    @Test
    public void testKafkaStreamsBuilder_Overrides() {
        BQRuntime runtime = testFactory
                .app("-c", "classpath:io/bootique/kafka/streams/KafkaStreamsModule_AllConfigsIT.yml")
                .autoLoadModules()
                .createRuntime();
        
        KafkaStreamsFactory factory = runtime.getInstance(KafkaStreamsFactory.class);
        DefaultKafkaStreamsBuilder builder = (DefaultKafkaStreamsBuilder) factory
                .topology(mock(Topology.class))
                .valueSerde(Serdes.IntegerSerde.class)
                .property(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName())
                .property(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());

        Properties mergedProps = builder.resolveProperties();
        // from YAML
        assertEquals("appid_from_yaml", mergedProps.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

        // from props
        assertEquals(Serdes.LongSerde.class.getName(), mergedProps.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG));

        // from calling 'valueSerde'
        assertEquals(Serdes.IntegerSerde.class.getName(), mergedProps.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG));
    }
}
