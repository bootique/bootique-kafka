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

package io.bootique.kafka.client;

import io.bootique.BQRuntime;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaConsumerRunner;
import io.bootique.kafka.client.producer.KafkaProducerFactory;
import io.bootique.test.junit.BQTestFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class KafkaClientIT {

    private static final String TEST_CLUSTER = "test_cluster";
    private static final String TEST_TOPIC = "test_topic";

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer()
            .withExternalZookeeper("zookeeper:2181");

    @ClassRule
    public static GenericContainer zookeeper = new GenericContainer("confluentinc/cp-zookeeper:4.0.0")
            .withNetwork(kafka.getNetwork())
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");

    @Rule
    public BQTestFactory testFactory = new BQTestFactory();

    private BQRuntime runtime;

    @Before
    public void before() {
        runtime  = testFactory
                .app("--config=classpath:config.yml")
                .property("bq.kafkaclient.clusters." + TEST_CLUSTER, kafka.getBootstrapServers())
                .module(KafkaClientModule.class)
                .createRuntime();
    }

    @Test
    public void testKafka() throws Exception {
        Producer<String, String> producer = runtime.getInstance(KafkaProducerFactory.class)
                .producer(new StringSerializer(), new StringSerializer())
                .cluster(TEST_CLUSTER)
                .create();
        producer.send(new ProducerRecord<>(TEST_TOPIC, "bootique", "kafka")).get();

        KafkaConsumerRunner<String, String> consumer = runtime.getInstance(KafkaConsumerFactory.class)
                .consumer(new StringDeserializer(), new StringDeserializer())
                .cluster(TEST_CLUSTER)
                .topics(TEST_TOPIC)
                .pollInterval(Duration.ofSeconds(1))
                .create();
        try {
            Iterator<ConsumerRecord<String, String>> iterator = consumer.iterator();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                assertTrue(iterator.hasNext());
                ConsumerRecord<String, String> record = iterator.next();
                assertEquals(record.key(), "bootique");
                assertEquals(record.value(), "kafka");
                return true;
            });
        } finally {
            consumer.close();
        }
    }
}
