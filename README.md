<!--
  Licensed to ObjectStyle LLC under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ObjectStyle LLC licenses
  this file to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
  -->

[![build test deploy](https://github.com/bootique/bootique-kafka/actions/workflows/maven.yml/badge.svg)](https://github.com/bootique/bootique-kafka/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.bootique.kafka/bootique-kafka-client.svg?colorB=brightgreen)](https://search.maven.org/artifact/io.bootique.kafka/bootique-kafka-client/)

# bootique-kafka

## Dependencies

Include the BOMs and then ```bootique-kafka-client```:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>3.0-M4</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
...

<!-- If using Producer and/or Consumer -->
<dependency>
	<groupId>io.bootique.kafka</groupId>
	<artifactId>bootique-kafka-client</artifactId>
</dependency>

<!-- If using streams -->
<dependency>
	<groupId>io.bootique.kafka</groupId>
	<artifactId>bootique-kafka-streams</artifactId>
</dependency>
```


## Producer/Consumer Configuration

Configure parameters in the YAML:

```yaml
kafkaclient:
  # any number of named clusters, specifying comma-separated bootstrap Kafka servers for each.
  clusters:
    cluster1: 127.0.0.1:9092
    cluster2: host1:9092,host2:9092
  consumer:
    autoCommit: true
    autoCommitInterval: "200ms"
    defaultGroup: myappgroup
    sessionTimeout: "2s"
  producer:
    acks: all # values are "all" or numeric number for min acks
    retries: 1
    batchSize: 16384
    linger: "1ms"
    bufferMemory: 33554432
```

Now you can inject producer and consumer factories and create any number of producers and consumers (for more details
see [bootique-kafka-examples](https://github.com/bootique-examples/bootique-kafka-examples)).

Producer:
```java
@Inject
KafkaProducerFactory factory;

public void runProducer() {

    Producer<byte[], String> producer = factory
        .charValueProducer()
        .cluster("cluster2")
        .create();

    producer.send(new ProducerRecord<>("mytopic", "Hi!"));

    // close if there's nothing else to send
    producer.close();
}
```

Consumer example (also see [this code sample](https://github.com/bootique-examples/bootique-kafka-consumer)) :
```java
@Inject
KafkaConsumerFactory factory;

// a custom function to consume data 
public void consumeBatch(Consumer<K, V> consumer, ConsumerRecords<K, V> data){
    data.forEach(r -> System.out.println(r.topic() + "_" + r.offset() + ": " + r.value()))
}

public void runConsumer() {
    KafkaPollingTracker poll = factory
        
        // configure consumer
        .charValueConsumer()
        .cluster("cluster1")
        .group("somegroup")
        .topic("mytopic")
        
        // start the consumer in the background
        .consume(this::consumeBatch, Duration.ofSeconds(1));
    
    // Close when we need to stop consumption. With no explicit Bootique will
    // close the consumer before the app exit
    // poll.close();
}
```

## Streams Configuration

TODO
