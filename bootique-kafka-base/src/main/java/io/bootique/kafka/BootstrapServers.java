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

package io.bootique.kafka;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * A value object representing Kafka bootstrap servers, which is a comma-separated String of server names with ports.
 *
 * @since 0.2
 */
@BQConfig
public class BootstrapServers {

    private String bootstrapServers;

    @BQConfigProperty
    public BootstrapServers(String bootstrapServers) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
    }

    public static BootstrapServers create(Collection<String> bootstrapServers) {
        if (Objects.requireNonNull(bootstrapServers).isEmpty()) {
            throw new IllegalArgumentException("Empty list of bootstrap servers");
        }

        return new BootstrapServers(String.join(",", bootstrapServers));
    }

    public String asString() {
        return bootstrapServers;
    }
}
