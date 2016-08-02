package io.bootique.kafka.client;

import com.nhl.bootique.test.junit.BQModuleProviderChecker;
import org.junit.Test;

public class KafkaClientModuleProviderTest {

    @Test
    public void testPresentInJar() {
        BQModuleProviderChecker.testPresentInJar(KafkaClientModuleProvider.class);
    }
}
