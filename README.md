[![Build Status](https://travis-ci.org/bootique/bootique-kafka-client.svg)](https://travis-ci.org/bootique/bootique-kafka-client)

# bootique-kafka-client

Integration of Kafka client for Bootique. Supports various versions of the Kafka client.


## Usage - Kafka Broker 0.8

First include the BOMs:
```xml
<!-- TODO: until these snapshots are released, they are available from -->
<!-- http://maven.objectstyle.org/nexus/content/repositories/bootique-snapshots/ -->
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.bootique.bom</groupId>
            <artifactId>bootique-io-bom</artifactId>
            <version>0.1-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        <dependency>
            <groupId>com.nhl.bootique.bom</groupId>
            <artifactId>bootique-bom</artifactId>
            <version>0.18-SNAPSHOT</version>
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
Now inject ```io.bootique.kafka.client_0_8.consumer.KafkaConsumerFactory``` in your code:
```java

@Inject
KafkaConsumerFactory consumerFactory;

public void doSomething() {
   consumerFactory.newConsumerConnector().createMessageStreams(..);
}
```