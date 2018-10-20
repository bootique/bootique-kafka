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
import io.bootique.kafka.BootstrapServersCollection;
import io.bootique.value.Duration;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 1.0.RC1
 */
@BQConfig
public class KafkaProducerFactoryFactory {

    private String acks;
    private Integer retries;
    private Integer batchSize;
    private Duration linger;
    private Integer bufferMemory;

    public KafkaProducerFactoryFactory() {
        this.bufferMemory = 33554432;
    }

    @BQConfigProperty
    public void setAcks(String acks) {
        this.acks = acks;
    }

    @BQConfigProperty
    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    @BQConfigProperty
    public void setBatchSize(Integer batchSize) {
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
    public void setBufferMemory(Integer bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public DefaultKafkaProducerFactory createProducer(BootstrapServersCollection clusters) {
        return new DefaultKafkaProducerFactory(clusters, createDefaultProperties());
    }

    protected Map<String, String> createDefaultProperties() {
        Map<String, String> properties = new HashMap<>();

        String acks = this.acks != null ? this.acks : "all";
        properties.put(ProducerConfig.ACKS_CONFIG, acks);

        int retries = this.retries != null ? this.retries : 0;
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(retries));

        int batchSize = this.batchSize != null ? this.batchSize : 16384;
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));

        long lingerMs = this.linger != null ? this.linger.getDuration().toMillis() : 0;
        properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));

        int bufferMemory = this.bufferMemory != null ? this.bufferMemory : 33554432;
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferMemory));

        return properties;
    }
}
