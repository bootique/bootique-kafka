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
import io.bootique.junit5.BQApp;
import io.bootique.junit5.BQTest;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaPoller;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@BQTest
public class KafkaConsumerFactory_KafkaPollerIT extends KafkaConsumerFactoryTestBase {

    @BQApp(skipRun = true)
    final BQRuntime app = createApp();

    @Test
    public void testConsume() {
        Producer<String, String> producer = createProducer(app);
        Map<String, String> data = new ConcurrentHashMap<>();
        String topic = getClass().getSimpleName() + "_testConsume_topic";
        String group = getClass().getSimpleName() + "_testConsume_group";

        try (KafkaPoller<?, ?> poller = app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                .cluster(TEST_CLUSTER)
                .group(group)
                .topics(topic)
                .consume((c, d) -> d.forEach(r -> data.put(r.key(), r.value())), Duration.ofSeconds(1))) {

            producer.send(new ProducerRecord<>(topic, "k1", "v1"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.containsKey("k1"));
            assertEquals(1, data.size(), () -> "Unexpected consumed data: " + data);
            assertEquals("v1", data.get("k1"));

            producer.send(new ProducerRecord<>(topic, "k2", "v2"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.containsKey("k2"));
            assertEquals(2, data.size(), () -> "Unexpected consumed data: " + data);
            assertEquals("v2", data.get("k2"));
        }
    }

    @Test
    public void testClose() throws InterruptedException {
        Producer<String, String> producer = createProducer(app);
        KafkaResourceManager resourceManager = app.getInstance(KafkaResourceManager.class);
        String topic = getClass().getSimpleName() + "_testClose_topic";
        String group = getClass().getSimpleName() + "_testClose_group";

        Map<String, String> data = new ConcurrentHashMap<>();

        try (KafkaPoller<?, ?> poller = app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                .cluster(TEST_CLUSTER)
                .group(group)
                .topics(topic)
                .consume((c, d) -> d.forEach(r -> data.put(r.key(), r.value())), Duration.ofSeconds(1))) {

            assertEquals(1, resourceManager.size());

            producer.send(new ProducerRecord<>(topic, "k1", "v1"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.containsKey("k1"));
            assertEquals(1, data.size(), () -> "Unexpected consumed data: " + data);
            assertEquals("v1", data.get("k1"));

            poller.close();

            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> resourceManager.size() == 0);

            assertEquals(0, resourceManager.size());
            producer.send(new ProducerRecord<>(topic, "k2", "v2"));

            Thread.sleep(1000L);
            assertFalse(data.containsKey("k2"));
        }
    }
}
