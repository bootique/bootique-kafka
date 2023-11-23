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
import io.bootique.kafka.client.consumer.KafkaConsumerBuilder;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaPollingTracker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

@BQTest
public class KafkaConsumerFactory_CallbackIT extends KafkaConsumerFactoryTestBase {

    @BQApp(skipRun = true)
    final BQRuntime app = createApp();

    @Test
    public void simpleConsume() {
        Producer<String, String> producer = createProducer(app);
        String topic = getClass().getSimpleName() + "_testConsume_topic";
        String group = getClass().getSimpleName() + "_testConsume_group";

        TestConsumer data = new TestConsumer(false);

        try (KafkaPollingTracker poller = app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                .cluster(TEST_CLUSTER)
                .group(group)
                .topics(topic)
                .consume(data, Duration.ofSeconds(1))) {

            producer.send(new ProducerRecord<>(topic, "k1", "v1"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.consumedKey("k1"));
            data.assertSize(1);
            data.assertKey("k1", "v1", 1);

            producer.send(new ProducerRecord<>(topic, "k2", "v2"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.consumedKey("k2"));
            data.assertSize(2);
            data.assertKey("k1", "v1", 1);
            data.assertKey("k2", "v2", 1);
        }
    }

    @Test
    public void close() throws InterruptedException {
        Producer<String, String> producer = createProducer(app);
        KafkaResourceManager resourceManager = app.getInstance(KafkaResourceManager.class);
        String topic = getClass().getSimpleName() + "_testClose_topic";
        String group = getClass().getSimpleName() + "_testClose_group";

        TestConsumer data = new TestConsumer(false);

        try (KafkaPollingTracker poller = app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                .cluster(TEST_CLUSTER)
                .group(group)
                .topics(topic)
                .consume(data, Duration.ofSeconds(1))) {

            assertEquals(1, resourceManager.size());

            producer.send(new ProducerRecord<>(topic, "k1", "v1"));
            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data.consumedKey("k1"));
            data.assertSize(1);
            data.assertKey("k1", "v1", 1);

            poller.close();

            Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> resourceManager.size() == 0);

            assertEquals(0, resourceManager.size());
            producer.send(new ProducerRecord<>(topic, "k2", "v2"));

            Thread.sleep(1000L);
            data.assertSize(1);
            data.assertKey("k1", "v1", 1);
        }
    }

    @Test
    public void atLeastOnceDelivery() throws InterruptedException {
        Producer<String, String> producer = createProducer(app);
        String topic = getClass().getSimpleName() + "_testAtLeastOnceDelivery_topic";
        String group = getClass().getSimpleName() + "_testAtLeastOnceDelivery_group";

        Supplier<KafkaConsumerBuilder<String, String>> consumerMaker = () -> app.getInstance(KafkaConsumerFactory.class)
                .charConsumer()
                // must disable auto-commit, as we'll be committing manually
                .autoCommit(false)
                // reducing this timeout from the default of 300000 speeds up rebalancing when consumers are disconnecting and reconnecting
                .property(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000")
                .cluster(TEST_CLUSTER)
                .group(group)
                .topics(topic);

        // no commit... the record above will be seen again by future consumers
        TestConsumer data1 = new TestConsumer(false);
        KafkaPollingTracker t1 = consumerMaker.get().consume(data1, Duration.ofSeconds(1));
        producer.send(new ProducerRecord<>(topic, "k1", "v1"));
        Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data1.consumedKey("k1"));
        data1.assertSize(1);
        data1.assertKey("k1", "v1", 1);
        t1.close(Duration.ofSeconds(1));


        // commit... the records consumed here should not be seen by future consumers
        TestConsumer data2 = new TestConsumer(true);
        KafkaPollingTracker t2 = consumerMaker.get().consume(data2, Duration.ofSeconds(1));
        producer.send(new ProducerRecord<>(topic, "k2", "v2"));
        Unreliables.retryUntilTrue(5, TimeUnit.SECONDS,
                () -> data2.consumedKey("k1") && data2.consumedKey("k2"));
        data2.assertSize(2);
        data2.assertKey("k1", "v1", 1);
        data2.assertKey("k2", "v2", 1);

        // TODO: a hack to let the commits to be processed. See TODO in KafkaPollingTrackerWorker.close(Duration)
        Thread.sleep(1000L);
        t2.close(Duration.ofSeconds(1));


        TestConsumer data3 = new TestConsumer(true);
        consumerMaker.get().consume(data3, Duration.ofSeconds(1));
        producer.send(new ProducerRecord<>(topic, "k3", "v3"));
        Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> data3.consumedKey("k3"));

        data3.assertSize(1);
        data3.assertKey("k3", "v3", 1);
    }
}
