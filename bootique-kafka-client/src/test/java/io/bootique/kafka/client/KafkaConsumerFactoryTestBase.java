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

import io.bootique.BQCoreModule;
import io.bootique.BQRuntime;
import io.bootique.Bootique;
import io.bootique.junit5.BQTest;
import io.bootique.kafka.client.producer.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@BQTest
public abstract class KafkaConsumerFactoryTestBase {

    protected static final String TEST_CLUSTER = "test_cluster";

    @Container
    protected final static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.3"));

    protected static BQRuntime createApp() {
        return Bootique
                .app("--config=classpath:config.yml")
                .modules(b -> BQCoreModule.extend(b).setProperty("bq.kafkaclient.clusters." + TEST_CLUSTER, kafka.getBootstrapServers()))
                .module(KafkaClientModule.class)
                .createRuntime();
    }

    protected Producer<String, String> createProducer(BQRuntime app) {
        return app.getInstance(KafkaProducerFactory.class)
                .producer(new StringSerializer(), new StringSerializer())
                .cluster(TEST_CLUSTER)
                .create();
    }
}
