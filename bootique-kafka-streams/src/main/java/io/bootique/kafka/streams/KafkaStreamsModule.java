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

package io.bootique.kafka.streams;

import io.bootique.BQModule;
import io.bootique.ModuleCrate;
import io.bootique.config.ConfigurationFactory;
import io.bootique.di.Binder;
import io.bootique.di.Provides;
import io.bootique.shutdown.ShutdownManager;

import javax.inject.Singleton;

public class KafkaStreamsModule implements BQModule {

    private static final String CONFIG_PREFIX = "kafkastreams";

    @Override
    public ModuleCrate crate() {
        return ModuleCrate.of(this)
                .description("Integrates Apache Kafka streams client")
                .config(CONFIG_PREFIX, KafkaStreamsFactoryFactory.class)
                .build();
    }

    @Override
    public void configure(Binder binder) {
    }

    @Provides
    @Singleton
    KafkaStreamsManager provideStreamsManager(ShutdownManager shutdownManager) {
        // we need a central stream manager that can track streams dynamically, but serve as the only integration
        // point with ShutdownManager
        return shutdownManager.onShutdown(new KafkaStreamsManager());
    }

    @Provides
    @Singleton
    KafkaStreamsFactory provideStreamsFactory(ConfigurationFactory configFactory, KafkaStreamsManager streamsManager) {
        return configFactory.config(KafkaStreamsFactoryFactory.class, CONFIG_PREFIX).createFactory(streamsManager);
    }
}
