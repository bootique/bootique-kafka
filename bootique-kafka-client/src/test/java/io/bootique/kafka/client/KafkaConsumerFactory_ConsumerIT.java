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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@BQTest
public class KafkaConsumerFactory_ConsumerIT extends KafkaConsumerFactoryTestBase {

    @BQApp(skipRun = true)
    final static BQRuntime app = createApp();

    @Test
    public void atLeastOnce_Delivery() {
        Producer<String, String> producer = createProducer(app);
        String topic = getClass().getSimpleName() + "_testAtLeastOnceDelivery_topic";
        String group = getClass().getSimpleName() + "_testAtLeastOnceDelivery_group";

        Supplier<Consumer<String, String>> consumerMaker = () -> app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                // must disable auto-commit, as we'll be committing manually
                .autoCommit(false)
                .cluster(TEST_CLUSTER)
                // reducing this timeout from the default of 300000 speeds up rebalancing when consumers are disconnecting and reconnecting
                .property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000")
                .topics(topic)
                .group(group)
                .createConsumer();


        // no commit... the record above will be seen again by future consumers
        TestConsumer data1 = new TestConsumer(false);
        try (Consumer<String, String> c1 = consumerMaker.get()) {
            producer.send(new ProducerRecord<>(topic, "k1", "v1"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {
                data1.consume(c1, c1.poll(Duration.ofSeconds(1)));
                return data1.consumedKey("k1");
            });
        }
        data1.assertSize(1);
        data1.assertKey("k1", "v1", 1);

        // commit... the records above should not be seen by future consumers
        TestConsumer data2 = new TestConsumer(true);
        try (Consumer<String, String> c2 = consumerMaker.get()) {
            producer.send(new ProducerRecord<>(topic, "k2", "v2"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {
                data2.consume(c2, c2.poll(Duration.ofSeconds(1)));
                return data2.consumedKey("k1") && data2.consumedKey("k2");
            });
        }
        data2.assertSize(2);
        data2.assertKey("k1", "v1", 1);
        data2.assertKey("k2", "v2", 1);

        TestConsumer data3 = new TestConsumer(true);
        try (Consumer<String, String> c3 = consumerMaker.get()) {
            producer.send(new ProducerRecord<>(topic, "k3", "v3"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {
                data3.consume(c3, c3.poll(Duration.ofSeconds(1)));
                return data3.consumedKey("k3");
            });
        }
        data3.assertSize(1);
        data3.assertKey("k3", "v3", 1);
    }
}
