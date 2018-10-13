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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BootstrapServersTest {

    protected <T> T deserialize(Class<T> type, String yml) throws IOException {
        YAMLParser parser = new YAMLFactory().createParser(yml);
        return new ObjectMapper().readValue(parser, type);
    }

    @Test
    public void testDeserializeString() throws IOException {
        BootstrapServers s = deserialize(BootstrapServers.class, "s1,s2");
        assertEquals("s1,s2", s.asString());
    }

    @Test
    public void testDeserializeInMap() throws IOException {
        C1 c1 = deserialize(C1.class, "servers:\n    a: s1,s2\n    b: s3:3001");
        assertEquals(2, c1.servers.size());
        assertEquals("s1,s2", c1.servers.get("a").asString());
        assertEquals("s3:3001", c1.servers.get("b").asString());
    }

    static class C1 {

        Map<String, BootstrapServers> servers;

        public void setServers(Map<String, BootstrapServers> servers) {
            this.servers = servers;
        }
    }
}
