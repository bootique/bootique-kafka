[![Build Status](https://travis-ci.org/bootique/bootique-kafka-client.svg)](https://travis-ci.org/bootique/bootique-kafka-client)

# bootique-kafka-client

Integration of Kafka client for Bootique. Supports various versions of the Kafka client.


## Usage - Kafka Broker 0.10 and Newer

Include the BOMs and then ```bootique-kafka-client```:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.19-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        <dependency>
            <groupId>com.nhl.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.19-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
...
<dependency>
	<groupId>io.bootique.kafka</groupId>
	<artifactId>bootique-kafka-client</artifactId>
</dependency>
```

Configure parameters in the YAML:

```yaml
kafka:
  # any number of named clusters, specifying comma-separated bootstrap Kafka servers for each.
  clusters:
    default: 127.0.0.1:9092
    other: host1:9092,host2:9092
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

```
Consumer example (also see [this code sample](https://github.com/bootique-examples/bootique-kafka-consumer)) :
```java

```


## Usage - Kafka Broker 0.8

_Legacy. Only includes consumer configuration._

Include the BOMs and then ```bootique-kafka-client-0.8```:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.18</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        <dependency>
            <groupId>com.nhl.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.18</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
...
<dependency>
	<groupId>io.bootique.kafka</groupId>
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