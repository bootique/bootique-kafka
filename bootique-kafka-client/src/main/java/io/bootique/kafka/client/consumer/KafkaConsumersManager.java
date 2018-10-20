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

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton that manages a mutable collection of Kafka Consumers, providing per instance start and close operations
 * as well as a collection close operation. Registered with Bootique {@link io.bootique.shutdown.ShutdownManager}. Since
 * explicitly-closed consumers are removed from the manager, we are able to prevent memory leaks if the app starts
 * and stops a lot of consumers.
 *
 * @since 1.0.RC1
 */
public class KafkaConsumersManager implements Closeable {

    private static final Duration CLOSE_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    private Map<Consumer<?, ?>, Integer> consumersMap;

    public KafkaConsumersManager() {
        this.consumersMap = new ConcurrentHashMap<>();
    }

    public void register(Consumer<?, ?> consumer) {
        consumersMap.put(consumer, 1);
    }

    /**
     * Wakes up the consumer to break out of polling Kafka. Expecting that "close" will be called when the
     * WakeupException (caused by attempt to poll after the wakeup), is caught inside KafkaConsumerRunner.
     *
     * @param consumer a consumer to wake up.
     */
    public void wakeup(Consumer<?, ?> consumer) {
        consumer.wakeup();
    }

    /**
     * Closes the consumer.
     *
     * @param consumer a consumer to close.
     */
    public void close(Consumer<?, ?> consumer) {
        consumer.close(CLOSE_TIMEOUT);
        consumersMap.remove(consumer);
    }

    @Override
    public void close() {
        consumersMap.keySet().forEach(this::wakeup);

        // TODO: unreliable ... how long do we have to wait for the wakeup to propagate and result in real close
        // being called?

        consumersMap.clear();
    }
}
