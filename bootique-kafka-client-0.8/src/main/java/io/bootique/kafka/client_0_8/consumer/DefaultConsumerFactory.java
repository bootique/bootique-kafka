/**
 *  Licensed to ObjectStyle LLC under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ObjectStyle LLC licenses
 *  this file to you under the Apache License, Version 2.0 (the
 *  “License”); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.bootique.kafka.client_0_8.consumer;

import kafka.consumer.Consumer;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class DefaultConsumerFactory implements ConsumerFactory {

    private Map<String, Map<String, String>> configs;

    public DefaultConsumerFactory(Map<String, Map<String, String>> configs) {
        this.configs = configs;
    }

    @Override
    public ConsumerConnector newConsumerConnector() {
        return newConsumerConnector(getDefaultName(), null);
    }

    @Override
    public ConsumerConnector newConsumerConnector(String name) {
        return newConsumerConnector(name, null);
    }

    @Override
    public ConsumerConnector newConsumerConnector(ConsumerConfig configOverrides) {
        return newConsumerConnector(getDefaultName(), configOverrides);
    }

    @Override
    public ConsumerConnector newConsumerConnector(String name, ConsumerConfig configOverrides) {

        Properties mergedProps = new Properties();

        Map<String, String> config = configs.get(name);

        if (config != null) {
            mergedProps.putAll(config);
        }

        if (configOverrides != null) {
            mergedProps.putAll(configOverrides.createConsumerConfig());
        }

        return Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(mergedProps));
    }

    public Collection<String> getConfigNames() {
        return Collections.unmodifiableCollection(configs.keySet());
    }

    private String getDefaultName() {
        Collection<String> allNames = configs.keySet();

        switch (allNames.size()) {
            case 0:
                throw new IllegalStateException("Kafka consumers are not configured");
            case 1:
                return allNames.iterator().next();
            default:
                throw new IllegalStateException("Default Kafka consumer name ambiguity. " +
                        "More then one consumer is provided in configuration.");
        }
    }
}
