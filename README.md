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

[![Build Status](https://travis-ci.org/bootique/bootique-kafka-client.svg)](https://travis-ci.org/bootique/bootique-kafka-client)
[![Maven Central](https://img.shields.io/maven-central/v/io.bootique.kafka.client/bootique-kafka-client.svg?colorB=brightgreen)](https://search.maven.org/artifact/io.bootique.kafka.client/bootique-kafka-client/)

# bootique-kafka-client

Integration of Kafka client for Bootique. Supports versions 0.8 and 0.10 of the Kafka client, as described below. The 
older 0.8 client requires Zookeeper connection for consumer. 0.10 bootstraps directly with Kafka.

See usage examples:
* [bootique-kafka-producer](https://github.com/bootique-examples/bootique-kafka-producer)
* [bootique-kafka-consumer](https://github.com/bootique-examples/bootique-kafka-consumer)

## Usage - Kafka Broker 0.10 and Newer

Include the BOMs and then ```bootique-kafka-client```:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.25</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
...
<dependency>
	<groupId>io.bootique.kafka.client</groupId>
	<artifactId>bootique-kafka-client</artifactId>
</dependency>
```

Configure parameters in the YAML. Note that practically all of these settings can be overidden when obtaining a 
specific Producer or Consumer instance via ```io.bootique.kafka.client.KafkaClientFactory```. So this is just a 
collection of defaults for the most typical Producer or Consumer:

```yaml
kafkaclient:
  # any number of named clusters, specifying comma-separated bootstrap Kafka servers for each.
  clusters:
    cluster1: 127.0.0.1:9092
    cluster2: host1:9092,host2:9092
  # Optional consumer configuration template
  consumer:
    autoCommit: true
    autoCommitIntervalMs: 200
    defaultGroup: myappgroup
    sessionTimeoutMs: 20000
  # Optional producer configuration template
  producer:
    acks: all # values are "all" or numeric number for min acks
    retries: 1
    batchSize: 16384
    lingerMs: 1
    bufferMemory: 33554432
```

Now you can inject ```io.bootique.kafka.client.KafkaClientFactory``` and request producers and consumers. Producer 
example (also see [this code sample](https://github.com/bootique-examples/bootique-kafka-producer)) :
```java
@Inject
KafkaClientFactory factory;

public void runProducer() {
    
    // not overriding any defaults here...
    ProducerConfig<byte[], String> config = ProducerConfig
        .charValueConfig()
        .build();
    
    Producer<byte[], String> producer = factory.createProducer("cluster2", config);
    producer.send(new ProducerRecord<>("mytopic", "Hi!"));
}
```
Consumer example (also see [this code sample](https://github.com/bootique-examples/bootique-kafka-consumer)) :
```java
@Inject
KafkaClientFactory factory;

public void runConsumer() {
    
    // overriding group default
    ConsumerConfig<byte[], String> config = ConsumerConfig
        .charValueConfig()
        .group("somegroup")
        .build();
    
    Consumer<byte[], String> consumer = factory.createConsumer("cluster1", config);
    consumer.subscribe(Collections.singletonList("mytopic"));
    while (true) {
        for (ConsumerRecord<byte[], String> r : consumer.poll(1000)) {
            System.out.println(r.topic() + "_" + r.partition() + "_" + r.offset() + ": " + r.value());
        }
    }
}
```

## Usage - Kafka Broker 0.8

_Legacy. Only provides consumer configuration and factory._

Include the BOMs and then ```bootique-kafka-client-0.8```:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.25</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
...
<dependency>
	<groupId>io.bootique.kafka.client</groupId>
	<artifactId>bootique-kafka-client-0.8</artifactId>
</dependency>
```
Now configure Zookeeper connection in YAML:
```yml
kafka:
  consumers:
     someConsumer:
       zookeeperConnect: 127.0.0.1:2181
       group: mygroup
```
You can inject ```io.bootique.kafka.client_0_8.consumer.ConsumerFactory``` in your code. It can be either used directly:
```java

@Inject
KafkaConsumerFactory consumerFactory;

public void doSomething() {
   consumerFactory.newConsumerConnector().createMessageStreams(..);
}
```

or via ```TopicConsumer``` API:

```java
@Inject
KafkaConsumerFactory consumerFactory;

ExecutorService executor = ...

try (TopicConsumer<K, V> consumer = createConsumer()) {

    try {
        kafkaConsumer.consumeAll(executor, this::process).get();
    } catch (InterruptedException | ExecutionException e) {
		e.printStackTrace();
    }
}

void process(K key, V message) {
    // do something
}

TopicConsumer<K, V> createConsumer() {
	return TopicConsumer
		.builder(keyDecoder, valueDecoder)
		.configName("someConsumer")
		.group("someGroup")
		.threads(2)
		.topic("my.topic")
		.build(consumerFactory.get());
}

```
