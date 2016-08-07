package io.bootique.kafka.client;

import com.google.inject.Module;
import io.bootique.BQModuleProvider;

/**
 * @since 0.2
 */
public class KafkaClientModuleProvider implements BQModuleProvider {

    @Override
    public Module module() {
        return new KafkaClientModule();
    }
}
