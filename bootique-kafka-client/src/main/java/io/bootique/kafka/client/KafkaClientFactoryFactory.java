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
import io.bootique.kafka.client.consumer.ConsumerFactory;
import io.bootique.kafka.client.producer.ProducerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A configuration object that describes a a set of Kafka servers  as well as a producer and consumer templates. Serves
 * as a factory for server producers and consumers.
 *
 * @since 0.2
 */
@BQConfig
public class KafkaClientFactoryFactory {

    private Map<String, BootstrapServers> clusters;
    private ConsumerFactory consumer;
    private ProducerFactory producer;

    public KafkaClientFactoryFactory() {
        this.consumer = new ConsumerFactory();
    }

    @BQConfigProperty
    public void setConsumer(ConsumerFactory consumer) {
        this.consumer = consumer;
    }

    @BQConfigProperty
    public void setProducer(ProducerFactory producer) {
        this.producer = producer;
    }

    public Map<String, BootstrapServers> getClusters() {
        return clusters;
    }

    @BQConfigProperty
    public void setClusters(Map<String, BootstrapServers> clusters) {
        this.clusters = clusters;
    }

    public DefaultKafkaClientFactory createFactory() {

        Map<String, BootstrapServers> clusters = this.clusters != null ? this.clusters : Collections.emptyMap();
        ConsumerFactory consumerTemplate = this.consumer != null ? this.consumer : new ConsumerFactory();
        ProducerFactory producerTemplate = this.producer != null ? this.producer : new ProducerFactory();

        return new DefaultKafkaClientFactory(clusters, consumerTemplate, producerTemplate);
    }
}
