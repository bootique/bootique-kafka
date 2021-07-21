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
package io.bootique.kafka.client.consumer;

import io.bootique.kafka.client.KafkaResourceManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

/**
 * @since 3.0.M1
 */
class KafkaPollingTrackerWorker<K, V> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPollingTrackerWorker.class);

    private final Consumer<K, V> consumer;
    private final KafkaConsumerCallback<K, V> callback;
    private final Duration pollInterval;
    private final KafkaResourceManager resourceManager;
    private final Runnable runAfterClose;

    protected KafkaPollingTrackerWorker(
            KafkaResourceManager resourceManager,
            Runnable runAfterClose,
            Consumer<K, V> consumer,
            KafkaConsumerCallback<K, V> callback,
            Duration pollInterval) {

        this.resourceManager = Objects.requireNonNull(resourceManager);
        this.consumer = Objects.requireNonNull(consumer);
        this.callback = Objects.requireNonNull(callback);
        this.pollInterval = Objects.requireNonNull(pollInterval);
        this.runAfterClose = Objects.requireNonNull(runAfterClose);
        resourceManager.register(this);
    }

    public void poll() {

        while (true) {
            ConsumerRecords<K, V> data;
            try {
                data = consumer.poll(pollInterval);
            } catch (WakeupException | InterruptException e) {
                break;
            }

            callback.consume(consumer, data);
        }
    }

    @Override
    public void close() {
        close(Duration.ZERO);
    }

    public void close(Duration timeout) {
        // allowing consumer to finish processing of the current batch
        LOGGER.info("Stopping consumer {}", System.identityHashCode(consumer));

        consumer.wakeup();
        resourceManager.unregister(this);

        LOGGER.info("Stopping consumer {}", System.identityHashCode(consumer));

        try {
            // closing with non-zero timeout gives Kafka a chance to commit offsets, etc.

            // TODO: our tests (KafkaConsumerFactory_CallbackIT) show that if consumer is sitting in a poll loop,
            //  commits can still be lost, and "consumer.close(timeout)" exits almost immediately, without processing
            //  pending commits. In the tests we had to insert an explicit delay before close
            consumer.close(timeout);
        } catch (Throwable th) {
            // ignoring...
        }

        try {
            runAfterClose.run();
        } catch (Throwable th) {
            // ignoring...
        }
    }
}
