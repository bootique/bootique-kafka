package io.bootique.kafka.client;

import io.bootique.BQRuntime;
import io.bootique.kafka.BootstrapServers;
import io.bootique.kafka.client.consumer.KafkaConsumerRunner;
import io.bootique.kafka.client.consumer.KafkaConsumersManager;
import io.bootique.test.junit.BQTestFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class KafkaContainerIT {

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Rule
    public BQTestFactory testFactory = new BQTestFactory();

    private BQRuntime runtime;

    @Before
    public void before() {
        runtime  = testFactory
                .app("--config=classpath:config.yml")
                .module(KafkaClientModule.class)
                .createRuntime();
    }

    @Test
    public void testExternalZookeeperWithExternalNetwork() throws Exception {

        try (
                KafkaContainer kafka = new KafkaContainer()
                        .withExternalZookeeper("zookeeper:2181");

                GenericContainer zookeeper = new GenericContainer("confluentinc/cp-zookeeper:4.0.0")
                        .withNetwork(kafka.getNetwork())
                        .withNetworkAliases("zookeeper")
                        .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
        ) {
            Stream.of(kafka, zookeeper).parallel().forEach(GenericContainer::start);

            String bootstrapServers = kafka.getBootstrapServers();
            testKafkaFunctionality(bootstrapServers);
        }
    }

    private void testKafkaFunctionality(String bootstrapServers) throws Exception {

        KafkaClientFactoryFactory instance = runtime.getInstance(KafkaClientFactoryFactory.class);
        KafkaConsumersManager kafkaConsumersManager = runtime.getInstance(KafkaConsumersManager.class);
        Map<String, BootstrapServers> stringStringMap = Collections.singletonMap(BOOTSTRAP_SERVERS_CONFIG,
                BootstrapServers.create(Collections.singletonList(bootstrapServers)));
        instance.setClusters(stringStringMap);
        String topicName = "mytopic";

        Producer<String, String> producer = instance.createProducerFactory()
                .producer(new StringSerializer(), new StringSerializer())
                .property(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers).create();

        KafkaConsumerRunner<String, String> consumerRunner = instance
                .createConsumerFactory(kafkaConsumersManager)
                .consumer(new StringDeserializer(), new StringDeserializer())
                .property(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .topics(topicName)
                .create();

        kafkaConsumersManager.register(consumerRunner.getConsumer());

        producer.send(new ProducerRecord<>(topicName, "bootique", "kafka")).get();

        Iterator<ConsumerRecord<String, String>> iterator = consumerRunner.iterator();
        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {

            assertTrue(iterator.hasNext());
            ConsumerRecord<String, String> record = iterator.next();
            assertEquals(record.key(), "bootique");
            assertEquals(record.value(), "kafka");
            return true;
        });

        consumerRunner.close();
    }
}
