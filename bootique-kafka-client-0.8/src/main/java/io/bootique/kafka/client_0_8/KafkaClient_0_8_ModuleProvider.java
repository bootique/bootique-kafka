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

package io.bootique.kafka.client_0_8;

import io.bootique.BQModuleMetadata;
import io.bootique.BQModuleProvider;
import io.bootique.di.BQModule;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

public class KafkaClient_0_8_ModuleProvider implements BQModuleProvider {

    @Override
    public BQModule module() {
        return new KafkaClient_0_8_Module();
    }

    @Override
    public Map<String, Type> configs() {
        // TODO: config prefix is hardcoded. Refactor away from ConfigModule, and make provider
        // generate config prefix, reusing it in metadata...
        return Collections.singletonMap("kafkaclient", KafkaClient_0_8_Factory.class);
    }

    @Override
    public BQModuleMetadata.Builder moduleBuilder() {
        return BQModuleProvider.super
                .moduleBuilder()
                .description("Provides integration with Apache Kafka client library, v.0.8");
    }
}
