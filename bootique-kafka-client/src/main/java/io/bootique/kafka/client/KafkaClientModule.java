package io.bootique.kafka.client;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.bootique.BQCoreModule;
import io.bootique.ConfigModule;
import io.bootique.config.ConfigurationFactory;
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
        BQCoreModule.extend(binder)
                .setLogLevel(ConsumerConfig.class.getName(), Level.WARNING)
                .setLogLevel(ProducerConfig.class.getName(), Level.WARNING);
    }

    @Singleton
    @Provides
    KafkaClientFactory provideClientFactory(ConfigurationFactory configFactory) {
        return configFactory.config(KafkaClientFactoryFactory.class, configPrefix).createFactory();
    }
}
