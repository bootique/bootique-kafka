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

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.BootstrapServersCollection;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaConsumerFactoryFactory;
import io.bootique.kafka.client.consumer.KafkaConsumersManager;
import io.bootique.kafka.client.producer.DefaultKafkaProducerFactory;
import io.bootique.kafka.client.producer.KafkaProducerFactoryFactory;

import java.util.Map;

/**
 * A configuration object that describes a a set of Kafka servers  as well as a producer and consumer templates. Serves
 * as a factory for server producers and consumers.
 */
@BQConfig
public class KafkaClientFactoryFactory {

    private Map<String, BootstrapServers> clusters;
    private KafkaConsumerFactoryFactory consumer;
    private KafkaProducerFactoryFactory producer;

    public KafkaClientFactoryFactory() {
        this.consumer = new KafkaConsumerFactoryFactory();
    }

    @BQConfigProperty
    public void setConsumer(KafkaConsumerFactoryFactory consumer) {
        this.consumer = consumer;
    }

    @BQConfigProperty
    public void setProducer(KafkaProducerFactoryFactory producer) {
        this.producer = producer;
    }

    /**
     * @return a new consumer factory.
     */
    public KafkaConsumerFactory createConsumerFactory(KafkaConsumersManager consumersManager) {
        return nonNullConsumer().createConsumer(consumersManager, getClusters());
    }

    /**
     * @return a new producer factory.
     */
    public DefaultKafkaProducerFactory createProducerFactory() {
        return nonNullProducer().createProducer(getClusters());
    }

    private KafkaConsumerFactoryFactory nonNullConsumer() {
        return consumer != null ? consumer : new KafkaConsumerFactoryFactory();
    }

    private KafkaProducerFactoryFactory nonNullProducer() {
        return this.producer != null ? this.producer : new KafkaProducerFactoryFactory();
    }

    private BootstrapServersCollection getClusters() {

        if (clusters == null || clusters.isEmpty()) {
            // should we use "localhost:9092" implicitly, or is it too confusing?
            throw new IllegalStateException("No 'clusters' configured for KafkaClient");
        }

        return new BootstrapServersCollection(clusters);
    }

    @BQConfigProperty
    public void setClusters(Map<String, BootstrapServers> clusters) {
        this.clusters = clusters;
    }
}
