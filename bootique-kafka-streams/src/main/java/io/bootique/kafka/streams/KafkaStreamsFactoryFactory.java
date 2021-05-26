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

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.BootstrapServersCollection;
import io.bootique.kafka.streams.config.ProcessingGuarantee;
import io.bootique.value.Bytes;
import org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


@BQConfig
public class KafkaStreamsFactoryFactory {

    private Map<String, BootstrapServers> clusters;
    private String applicationId;
    private File stateDir;
    private Bytes cacheMaxBytesBuffering;
    private ProcessingGuarantee processingGuarantee;

    // TODO: other StreamsConfig.*_CONFIG properties
    // TODO: properties for adminClient, consumer, globalConsumer, producer, restoreConsumer

    public DefaultKafkaStreamsFactory createFactory(KafkaStreamsManager streamsManager) {
        return new DefaultKafkaStreamsFactory(streamsManager, getClusters(), createProperties());
    }

    protected Map<String, String> createProperties() {
        Map<String, String> properties = new HashMap<>();

        // Note that below we are converting all values to Strings. Kafka would probably work if we preserve some of them
        // as ints/longs/etc, but let's honor an implied contract of the Properties class - its values must be Strings.

        if (applicationId != null) {
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        }

        if (cacheMaxBytesBuffering != null) {
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, String.valueOf(cacheMaxBytesBuffering.getBytes()));
        }

        if (processingGuarantee != null) {
            properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee.name());
        }

        if (stateDir != null) {
            properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        }

        // TODO: load consumer/producer/etc properties...

        return properties;
    }

    private BootstrapServersCollection getClusters() {

        if (clusters == null || clusters.isEmpty()) {
            // should we use "localhost:9092" implicitly, or is it too confusing?
            throw new IllegalStateException("No 'clusters' configured for KafkaStreams");
        }

        return new BootstrapServersCollection(clusters);
    }

    @BQConfigProperty
    public void setClusters(Map<String, BootstrapServers> clusters) {
        this.clusters = clusters;
    }

    @BQConfigProperty("An identifier for the stream processing application. Must be unique within the Kafka cluster. " +
            "Used as the default client-id prefix, the group-id for membership management and the changelog topic prefix.")
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @BQConfigProperty("Maximum number of memory bytes to be used for buffering across all threads")
    public void setCacheMaxBytesBuffering(Bytes cacheMaxBytesBuffering) {
        this.cacheMaxBytesBuffering = cacheMaxBytesBuffering;
    }

    @BQConfigProperty("The processing guarantee that should be used. Possible values are 'at_least_once' (default) and " +
            "'exactly_once'. 'exactly-once' processing requires a cluster of at least three brokers by default what is " +
            "the recommended setting for production; for development you can change this, by adjusting broker setting " +
            "`transaction.state.log.replication.factor`."
    )
    public void setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
    }

    @BQConfigProperty
    public void setStateDir(File stateDir) {
        this.stateDir = stateDir;
    }
}
