package io.bootique.kafka.streams;

import io.bootique.junit5.BQModuleProviderChecker;
import org.junit.jupiter.api.Test;

public class KafkaStreamsModuleProviderTest {

    @Test
    public void testAutoLoadable() {
        BQModuleProviderChecker.testAutoLoadable(KafkaStreamsModuleProvider.class);
    }
}
