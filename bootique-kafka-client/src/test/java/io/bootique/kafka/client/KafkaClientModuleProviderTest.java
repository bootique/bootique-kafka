package io.bootique.kafka.client;

import io.bootique.test.junit.BQModuleProviderChecker;
import org.junit.Test;

public class KafkaClientModuleProviderTest {

    @Test
    public void testPresentInJar() {
        BQModuleProviderChecker.testPresentInJar(KafkaClientModuleProvider.class);
    }

    @Test
    public void testMetadata() {
        BQModuleProviderChecker.testMetadata(KafkaClientModuleProvider.class);
    }
}
