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

package io.bootique.kafka.client.producer;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.client.FactoryUtils;
import io.bootique.value.Duration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.bootique.kafka.client.FactoryUtils.setProperty;

/**
 * @since 0.2
 */
@BQConfig
public class ProducerFactory {

    private static final String BOOTSTRAP_SERVERS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String ACKS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
    private static final String RETRIES_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
    private static final String BATCH_SIZE_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
    private static final String LINGER_MS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
    private static final String BUFFER_MEMORY_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;


    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);

    private String acks;
    private int retries;
    private int batchSize;
    private Duration linger;
    private int bufferMemory;

    public ProducerFactory() {

        this.acks = "all";
        this.retries = 0;
        this.batchSize = 16384;
        this.bufferMemory = 33554432;
    }

    @BQConfigProperty
    public void setAcks(String acks) {
        this.acks = acks;
    }

    @BQConfigProperty
    public void setRetries(int retries) {
        this.retries = retries;
    }

    @BQConfigProperty
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @BQConfigProperty("The producer groups together any records that arrive in between request transmissions into a single batched request. "
            + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
            + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
            + "of artificial delay, that is, rather than immediately sending out a record the producer will wait for up to "
            + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
            + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
            + "we get 'batchSize' worth of records for a partition it will be sent immediately regardless of this "
            + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
            + "specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay).")
    public void setLinger(Duration linger) {
        this.linger = linger;
    }

    @BQConfigProperty("The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are " +
            "sent faster than they can be delivered to the server the producer will block for \" + MAX_BLOCK_MS_CONFIG + \" after which it " +
            "will throw an exception. This setting should correspond roughly to the total memory the producer will use.")
    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public <K, V> Producer<K, V> createProducer(BootstrapServers bootstrapServers, ProducerConfig<K, V> config) {

        Map<String, Object> properties = new HashMap<>();

        // TODO: replace FactoryUtils with consumer and streams style property merging...
        FactoryUtils.setRequiredProperty(properties,
                BOOTSTRAP_SERVERS_CONFIG,
                Objects.requireNonNull(bootstrapServers).asString());

        setProperty(properties,
                ACKS_CONFIG,
                config.getAcks(),
                acks);
        setProperty(properties,
                RETRIES_CONFIG,
                config.getRetries(),
                retries);
        setProperty(properties,
                BATCH_SIZE_CONFIG,
                config.getBatchSize(),
                batchSize);
        setProperty(properties,
                LINGER_MS_CONFIG,
                config.getLingerMs(),
                getLingerMs());
        setProperty(properties,
                BUFFER_MEMORY_CONFIG,
                config.getBufferMemory(),
                bufferMemory);


        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Creating producer bootstrapping with %s.",
                    properties.get(BOOTSTRAP_SERVERS_CONFIG)));
        }

        return new KafkaProducer<>(properties, config.getKeySerializer(), config.getValueSerializer());
    }

    private int getLingerMs() {
        return linger != null ? (int) linger.getDuration().toMillis() : 0;
    }

}
