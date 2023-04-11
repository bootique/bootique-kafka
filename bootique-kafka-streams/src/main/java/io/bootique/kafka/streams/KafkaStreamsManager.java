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

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton that manages a mutable collection of KafkaStreams, providing per instance start and close operations as
 * well as a collection close operation. Registered with Bootique {@link io.bootique.shutdown.ShutdownManager}. Since
 * explicitly-closed streams are removed from the manager, we are able to prevent memory leaks if the app starts
 * and stops a lot of streams.
 */
public class KafkaStreamsManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsManager.class);
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);

    private Map<KafkaStreams, Integer> streamsMap;

    public KafkaStreamsManager() {
        this.streamsMap = new ConcurrentHashMap<>();
    }

    public void start(KafkaStreams streams) {
        streamsMap.put(streams, 1);
        streams.start();
    }

    public void close(KafkaStreams streams) {
        streams.close(DEFAULT_CLOSE_TIMEOUT);
        streamsMap.remove(streams);
    }

    @Override
    public void close() {
        streamsMap.keySet().forEach(s -> {
            try {
                s.close(DEFAULT_CLOSE_TIMEOUT);
            } catch (Exception e) {
                // errors or timeouts stopping a given stream should not prevent us from stopping others
                LOGGER.warn("Error shutting down KafkaStreams. Ignoring.", e);
            }
        });

        streamsMap.clear();
    }


}
