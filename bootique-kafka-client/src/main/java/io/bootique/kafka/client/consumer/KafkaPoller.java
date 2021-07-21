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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A utility class that encapsulates Kafka data consumption flow.
 *
 * @since 3.0.M1
 */
public class KafkaPoller<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPoller.class);

    private final Consumer<K, V> consumer;
    private final KafkaConsumerCallback<K, V> callback;
    private final Duration pollInterval;

    private ExecutorService threadPool;

    public KafkaPoller(
            Consumer<K, V> consumer,
            KafkaConsumerCallback<K, V> callback,
            Duration pollInterval) {

        this.consumer = Objects.requireNonNull(consumer);
        this.callback = Objects.requireNonNull(callback);
        this.pollInterval = Objects.requireNonNull(pollInterval);
    }

    public void start() {
        if (isStarted()) {
            throw new IllegalStateException("Already running, can't start again");
        }

        // since the consumer will occupy the pool thread for a long period of time, there's no point in a shared
        // thread pool. We can manage our own single-threaded pool instead
        this.threadPool = Executors.newSingleThreadExecutor(new ConsumerThreadFactory());
        threadPool.submit(this::pollBlocking);
    }

    public void stop() {
        if (isStarted()) {
            // allowing consumer to finish processing of the current batch
            consumer.wakeup();
        }
    }

    protected boolean isStarted() {
        return threadPool != null;
    }

    protected void pollBlocking() {
        try {
            while (true) {
                ConsumerRecords<K, V> data = consumer.poll(pollInterval);
                callback.consume(consumer, data);
            }
        } catch (WakeupException e) {
            LOGGER.debug("Consumer polling is stopped");
            forceStop();
        }
    }

    protected void forceStop() {

        ExecutorService threadPool = this.threadPool;
        this.threadPool = null;

        try {
            consumer.close(pollInterval);
        } catch (Throwable th) {
            // ignoring...
        }

        if (threadPool != null) {
            try {
                threadPool.shutdownNow();
            } catch (Throwable th) {
                // ignoring
            }
        }
    }
}
