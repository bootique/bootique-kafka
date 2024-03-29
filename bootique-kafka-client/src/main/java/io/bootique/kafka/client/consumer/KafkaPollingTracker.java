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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Runs background callback-style Kafka message consumption loop. The only public API in this class is {@link #close()}
 * that allows to cleanly stop the background consumer.
 *
 * @since 3.0
 */
public class KafkaPollingTracker implements AutoCloseable {

    private final KafkaPollingTrackerWorker<?, ?> worker;
    private final KafkaResourceManager resourceManager;

    private ExecutorService threadPool;

    protected <K, V> KafkaPollingTracker(
            KafkaResourceManager resourceManager,
            Consumer<K, V> consumer,
            KafkaConsumerCallback<K, V> callback,
            Duration pollInterval) {

        this.resourceManager = Objects.requireNonNull(resourceManager);

        // since the consumer will occupy a pool thread for a long period of time, there's no point in a shared
        // thread pool. We can manage our own single-threaded pool instead
        this.threadPool = Executors.newSingleThreadExecutor(new ConsumerThreadFactory());
        this.worker = new KafkaPollingTrackerWorker<>(
                this::stopThreadPool,
                consumer,
                callback,
                pollInterval);

        start();
    }

    protected void start() {
        this.resourceManager.register(this);
        this.threadPool.submit(worker::poll);
    }

    @Override
    public void close() {
        close(Duration.ZERO);
    }

    public void close(Duration timeout) {
        if (isRunning()) {
            resourceManager.unregister(this);
            worker.close(timeout);
        }
    }

    protected boolean isRunning() {
        return threadPool != null;
    }

    protected void stopThreadPool() {

        ExecutorService threadPool = this.threadPool;
        this.threadPool = null;

        if (threadPool != null) {
            try {
                threadPool.shutdownNow();
            } catch (Throwable th) {
                // ignoring
            }
        }
    }
}
