/**
 *    Licensed to the ObjectStyle LLC under one
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConsumerConfig {

    private long zookeeperSessionTimeoutMs;
    private long zookeeperSyncTimeMs;
    private long autoCommitIntervalMs;
    private OffsetReset autoOffsetReset = OffsetReset.largest;
    private String group;

    public Map<String, String> createConsumerConfig() {

        Map<String, String> props = new HashMap<>();

        props.put("auto.offset.reset", Objects.requireNonNull(autoOffsetReset).name());

        if (group != null) {
            props.put("group.id", group);
        }

        if (zookeeperSessionTimeoutMs > 0) {
            props.put("zookeeper.session.timeout.ms", String.valueOf(zookeeperSessionTimeoutMs));
        }

        if (zookeeperSyncTimeMs > 0) {
            props.put("zookeeper.sync.time.ms", String.valueOf(zookeeperSyncTimeMs));
        }

        if (autoCommitIntervalMs > 0) {
            props.put("auto.commit.interval.ms", String.valueOf(autoCommitIntervalMs));
        }

        return props;
    }

    public void setAutoOffsetReset(OffsetReset autoOffsetReset) {
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset);
    }

    public void setZookeeperSessionTimeoutMs(long zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public void setZookeeperSyncTimeMs(long zookeeperSyncTimeMs) {
        this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public void setGroup(String group) {
        this.group = group;
    }

}
