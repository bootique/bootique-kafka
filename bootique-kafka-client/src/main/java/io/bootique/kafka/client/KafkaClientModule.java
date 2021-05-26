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
import io.bootique.ConfigModule;
import io.bootique.config.ConfigurationFactory;
import io.bootique.di.Binder;
import io.bootique.di.Provides;
import io.bootique.kafka.client.consumer.KafkaConsumerFactory;
import io.bootique.kafka.client.consumer.KafkaConsumersManager;
import io.bootique.kafka.client.producer.KafkaProducerFactory;
import io.bootique.shutdown.ShutdownManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.inject.Singleton;
import java.util.logging.Level;

public class KafkaClientModule extends ConfigModule {

    @Override
    public void configure(Binder binder) {
        // turn off chatty logs by default
        BQCoreModule.extend(binder)
                .setLogLevel(ConsumerConfig.class.getName(), Level.WARNING)
                .setLogLevel(ProducerConfig.class.getName(), Level.WARNING);
    }

    @Singleton
    @Provides
    KafkaClientFactoryFactory provideClientFactoryFactory(ConfigurationFactory configFactory) {
        return config(KafkaClientFactoryFactory.class, configFactory);
    }

    @Singleton
    @Provides
    KafkaProducerFactory provideClientFactory(KafkaClientFactoryFactory parentFactory) {
        return parentFactory.createProducerFactory();
    }

    @Singleton
    @Provides
    KafkaConsumersManager provideConsumersManager(ShutdownManager shutdownManager) {
        KafkaConsumersManager cm = new KafkaConsumersManager();
        shutdownManager.addShutdownHook(cm);
        return cm;
    }

    @Singleton
    @Provides
    KafkaConsumerFactory provideConsumerFactory(KafkaConsumersManager consumersManager, KafkaClientFactoryFactory parentFactory) {
        return parentFactory.createConsumerFactory(consumersManager);
    }
}
