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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A wrapper for a Kafka {@link org.apache.kafka.clients.consumer.Consumer} that provides an Iterator and Stream APIs
 * to consume data from topics. Behind the scenes manages consumer subscriptions, Kafka polling and Bootique shutdown
 * sequence. Just like the underlying Consumer, this wrapper is <i>single-threaded</i>.
 *
 * <p>WARNING: This object is rather limited and will only work properly in the offset auto-commit mode. E.g. it is
 * not suitable for "at least once" consumption. Consider using Kafka <code>Consumer</code> instead, that can be
 * obtained via {@link KafkaConsumerBuilder#createConsumer()}</p>
 */
public class KafkaConsumerRunner<K, V> implements Iterable<ConsumerRecord<K, V>> {

    private KafkaConsumersManager consumersManager;
    private Consumer<K, V> consumer;
    private Collection<String> topics;
    private Duration pollInterval;

    public KafkaConsumerRunner(
            KafkaConsumersManager consumersManager,
            Consumer<K, V> consumer,
            Collection<String> topics,
            Duration pollInterval) {

        this.consumersManager = consumersManager;
        this.consumer = consumer;
        this.topics = topics;
        this.pollInterval = pollInterval;
        this.consumersManager.register(consumer);
    }

    /**
     * Returns a semi-infinite stream of records from the underlying topic(s). Stops when the underlying consumer is
     * "woken up" or closed.
     */
    public Stream<ConsumerRecord<K, V>> stream() {
        return StreamSupport.stream(this.spliterator(), false);
    }

    /**
     * Returns a semi-infinite iterator of records from the underlying topic(s). Stops when the underlying consumer is
     * "woken up" or closed.
     */
    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new ConsumeIterator();
    }

    /**
     * Returns the underlying consumer for the users who need access to its low-level API.
     *
     * @return the underlying consumer for the users who need access to its low-level API.
     */
    public Consumer<K, V> getConsumer() {
        return consumer;
    }

    public void close() {
        // only wake up the consumer... Allow it to process the current batch, and terminate on next "poll" via a
        // WakeupException..
        consumersManager.wakeup(consumer);
    }

    protected class ConsumeIterator implements Iterator<ConsumerRecord<K, V>> {

        private Iterator<ConsumerRecord<K, V>> buffer;
        private boolean running;

        ConsumeIterator() {
            consumer.subscribe(topics);
            buffer = nextBatch();
            running = true;
        }

        @Override
        public boolean hasNext() {

            // this is an infinite iterator, until it is stopped
            if (!running) {
                return false;
            }

            // fetch the next buffer in "hasNext()"... Getting it in "next" will result in a broken iterator state if
            // the underlying consumer got closed.

            if (!buffer.hasNext()) {
                while (buffer != null && !buffer.hasNext()) {
                    // Note: this will cause data loss, as we are in auto-commit mode. So this is not suitable for
                    // "at least once" consumption mode, as mentioned in the Javadocs above
                    buffer = nextBatch();
                }
            }

            // we may have been shutdown while processing the loop above, so don't assume "true" here.
            return running;
        }

        @Override
        public ConsumerRecord<K, V> next() {
            checkStopped();
            return buffer.next();
        }

        protected void checkStopped() {
            if (!running) {
                throw new NoSuchElementException("Can't read more records. The Consumer was stopped.");
            }
        }

        protected Iterator<ConsumerRecord<K, V>> nextBatch() {
            try {
                return consumer.poll(pollInterval).iterator();
            } catch (WakeupException | InterruptException e) {
                running = false;
                consumersManager.close(consumer);
                return null;
            }
        }
    }
}
