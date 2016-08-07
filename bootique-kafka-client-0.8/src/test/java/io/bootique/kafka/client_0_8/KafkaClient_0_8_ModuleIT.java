package io.bootique.kafka.client_0_8;

import io.bootique.test.BQTestRuntime;
import io.bootique.test.junit.BQTestFactory;
import io.bootique.kafka.client_0_8.consumer.ConsumerFactory;
import io.bootique.kafka.client_0_8.consumer.DefaultConsumerFactory;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KafkaClient_0_8_ModuleIT {

    @Rule
    public BQTestFactory testFactory = new BQTestFactory();

    @Test
    public void testConsumerFactory_NoConfig() {
        BQTestRuntime runtime  = testFactory
                .newRuntime()
                .configurator(bootique -> bootique.module(KafkaClient_0_8_Module.class))
                .build();

        ConsumerFactory factory = runtime.getRuntime().getInstance(ConsumerFactory.class);
        assertNotNull(factory);
        assertTrue(factory instanceof DefaultConsumerFactory);
        assertEquals(0, ((DefaultConsumerFactory) factory).getConfigNames().size());
    }

    @Test
    public void testConsumerFactory_Consumers() {
        BQTestRuntime runtime  = testFactory
                .newRuntime()
                .configurator(bootique -> bootique.module(KafkaClient_0_8_Module.class))
                .build("--config=classpath:test1.yml");

        ConsumerFactory factory = runtime.getRuntime().getInstance(ConsumerFactory.class);
        assertNotNull(factory);
        assertTrue(factory instanceof DefaultConsumerFactory);

        assertArrayEquals(new String[] {"x", "y"}, ((DefaultConsumerFactory) factory).getConfigNames().toArray());
    }
}
