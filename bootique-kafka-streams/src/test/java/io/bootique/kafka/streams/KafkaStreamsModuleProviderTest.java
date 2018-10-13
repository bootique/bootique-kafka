package io.bootique.kafka.streams;

import io.bootique.test.junit.BQModuleProviderChecker;
import org.junit.Test;

public class KafkaStreamsModuleProviderTest {

    @Test
    public void testAutoLoadable() {
        BQModuleProviderChecker.testAutoLoadable(KafkaStreamsModuleProvider.class);
    }
}
