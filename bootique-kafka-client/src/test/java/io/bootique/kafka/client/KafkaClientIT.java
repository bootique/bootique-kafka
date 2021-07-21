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

import io.bootique.BQCoreModule;
import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.junit5.BQApp;
import io.bootique.junit5.BQTest;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaConsumerRunner;
import io.bootique.kafka.client.producer.KafkaProducerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@BQTest
public class KafkaClientIT {

    private static final String TEST_CLUSTER = "test_cluster";

    @Container
    final static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.3"));

    // TODO: have to start the app in the instance scope, to ensure Kafka container in the static scope was started
    //   How do we ensure Kafka startup when the app is in the static context?
    @BQApp(skipRun = true)
    final BQRuntime app = Bootique
            .app("--config=classpath:config.yml")
            .modules(b -> BQCoreModule.extend(b).setProperty("bq.kafkaclient.clusters." + TEST_CLUSTER, kafka.getBootstrapServers()))
            .module(KafkaClientModule.class)
            .createRuntime();

    @Test
    public void testConsumer_AtLeastOnce_Delivery() throws Exception {
        Producer<String, String> producer = app.getInstance(KafkaProducerFactory.class)
                .producer(new StringSerializer(), new StringSerializer())
                .cluster(TEST_CLUSTER)
                .create();

        Supplier<Consumer<String, String>> consumerMaker = () -> app.getInstance(KafkaConsumerFactory.class)
                .consumer(new StringDeserializer(), new StringDeserializer())
                // must disable auto-commit, as we'll be committing manually
                .autoCommit(false)
                .cluster(TEST_CLUSTER)
                .topics("topic1")
                .group("group1")
                .createConsumer();

        Consumer<String, String> c1 = consumerMaker.get();
        try {
            producer.send(new ProducerRecord<>("topic1", "k1", "v1")).get();
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {

                ConsumerRecords<String, String> rs = c1.poll(Duration.ofSeconds(1));
                assertEquals(1, rs.count());
                ConsumerRecord<String, String> r1 = rs.iterator().next();
                assertEquals("k1", r1.key());
                assertEquals("v1", r1.value());

                // no commit... the record above will be seen again by future consumers

                return true;
            });
        } finally {
            c1.close();
        }

        Consumer<String, String> c2 = consumerMaker.get();
        try {

            producer.send(new ProducerRecord<>("topic1", "k2", "v2")).get();

            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {

                ConsumerRecords<String, String> rs = c2.poll(Duration.ofSeconds(1));

                assertEquals(2, rs.count());
                Iterator<ConsumerRecord<String, String>> it = rs.iterator();
                ConsumerRecord<String, String> r1 = it.next();
                assertEquals("k1", r1.key());
                assertEquals("v1", r1.value());

                ConsumerRecord<String, String> r2 = it.next();
                assertEquals("k2", r2.key());
                assertEquals("v2", r2.value());

                // commit... the records above should not be seen by future consumers
                c2.commitSync();

                return true;
            });
        } finally {
            c2.close();
        }

        Consumer<String, String> c3 = consumerMaker.get();
        try {

            producer.send(new ProducerRecord<>("topic1", "k3", "v3")).get();

            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {

                ConsumerRecords<String, String> rs = c3.poll(Duration.ofSeconds(1));
                assertEquals(1, rs.count());
                Iterator<ConsumerRecord<String, String>> it = rs.iterator();
                ConsumerRecord<String, String> r3 = it.next();
                assertEquals("k3", r3.key());
                assertEquals("v3", r3.value());

                c3.commitSync();

                return true;
            });
        } finally {
            c3.close();
        }
    }

    @Test
    public void testConsumerRunner() throws Exception {
        Producer<String, String> producer = app.getInstance(KafkaProducerFactory.class)
                .producer(new StringSerializer(), new StringSerializer())
                .cluster(TEST_CLUSTER)
                .create();
        producer.send(new ProducerRecord<>("topic2", "bootique", "kafka")).get();

        KafkaConsumerRunner<String, String> consumer = app.getInstance(KafkaConsumerFactory.class)
                .consumerRunner(new StringDeserializer(), new StringDeserializer())
                .cluster(TEST_CLUSTER)
                .group("group2")
                .topics("topic2")
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
