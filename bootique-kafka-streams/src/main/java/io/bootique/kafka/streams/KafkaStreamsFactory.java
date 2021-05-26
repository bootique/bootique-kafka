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

import org.apache.kafka.streams.Topology;

/**
 * An injectable factory of the custom KafkaStreams. Streams are configured via a special builder
 * (see {@link #topology(Topology)} method) and returned wrapped in {@link KafkaStreamsRunner}.
 */
public interface KafkaStreamsFactory {

    /**
     * Returns a builder of {@link KafkaStreamsRunner} for a given topology.
     *
     * @param topology streams topology to use in the stream being built.
     * @return an instance of {@link KafkaStreamsBuilder} that can be further customized by the caller and then used
     * to create a {@link KafkaStreamsRunner}.
     */
    KafkaStreamsBuilder topology(Topology topology);
}
