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

import io.bootique.kafka.client.consumer.KafkaConsumerCallback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class TestConsumer implements KafkaConsumerCallback<String, String> {

    private final boolean commit;
    private final Map<String, List<String>> consumed;
    private int count;

    public TestConsumer(boolean commit) {
        this.commit = commit;
        this.consumed = new ConcurrentHashMap<>();
    }

    @Override
    public void consume(Consumer<String, String> consumer, ConsumerRecords<String, String> data) {

        count += data.count();

        data.forEach(r -> consumed.computeIfAbsent(r.key(), k -> new CopyOnWriteArrayList<>()).add(r.value()));

        if (commit) {
            consumer.commitSync();
        }
    }

    public boolean consumedKey(String key) {
        return consumed.containsKey(key);
    }

    public void assertSize(int expectedSize) {
        assertEquals(expectedSize, count);
    }

    public void assertKey(String key, String value, int seen) {
        List<String> vals = consumed.get(key);

        assertNotNull(vals);
        assertEquals(seen, vals.size());

        Set<String> unique = new HashSet<>(vals);
        assertEquals(1, unique.size());
        assertTrue(unique.contains(value));
    }
}
