package io.bootique.kafka.client_0_8;

import io.bootique.test.junit.BQModuleProviderChecker;
import org.junit.Test;

public class KafkaClient_0_8_ModuleProviderTest {

    @Test
    public void testAutoLoadable() {
        BQModuleProviderChecker.testAutoLoadable(KafkaClient_0_8_ModuleProvider.class);
    }

    @Test
    public void testMetadata() {
        BQModuleProviderChecker.testMetadata(KafkaClient_0_8_ModuleProvider.class);
    }
}
