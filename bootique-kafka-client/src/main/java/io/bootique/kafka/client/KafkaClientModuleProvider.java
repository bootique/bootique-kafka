package io.bootique.kafka.client;

import com.google.inject.Module;
import com.nhl.bootique.BQModuleProvider;

/**
 * @since 0.2
 */
public class KafkaClientModuleProvider implements BQModuleProvider {

    @Override
    public Module module() {
        return new KafkaClientModule();
    }
}
