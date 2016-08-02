package io.bootique.kafka.client;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.nhl.bootique.ConfigModule;
import com.nhl.bootique.config.ConfigurationFactory;

/**
 * @since 0.2
 */
public class KafkaClientModule extends ConfigModule {

    @Singleton
    @Provides
    KafkaClientFactory provideClientFactory(ConfigurationFactory configFactory) {
        return configFactory.config(KafkaClientFactoryFactory.class, configPrefix).createFactory();
    }
}
