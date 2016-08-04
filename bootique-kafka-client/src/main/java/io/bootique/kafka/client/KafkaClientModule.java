package io.bootique.kafka.client;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.nhl.bootique.BQCoreModule;
import com.nhl.bootique.ConfigModule;
import com.nhl.bootique.config.ConfigurationFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.logging.Level;

/**
 * @since 0.2
 */
public class KafkaClientModule extends ConfigModule {

    @Override
    public void configure(Binder binder) {
        // turn off chatty logs by default
        BQCoreModule.contributeLogLevels(binder)
                .addBinding(ConsumerConfig.class.getName())
                .toInstance(Level.WARNING);
        BQCoreModule.contributeLogLevels(binder)
                .addBinding(ProducerConfig.class.getName())
                .toInstance(Level.WARNING);
    }

    @Singleton
    @Provides
    KafkaClientFactory provideClientFactory(ConfigurationFactory configFactory) {
        return configFactory.config(KafkaClientFactoryFactory.class, configPrefix).createFactory();
    }
}
