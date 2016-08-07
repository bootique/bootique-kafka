package io.bootique.kafka.client_0_8;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.bootique.ConfigModule;
import io.bootique.config.ConfigurationFactory;
import io.bootique.kafka.client_0_8.consumer.ConsumerFactory;

public class KafkaClient_0_8_Module extends ConfigModule {

// TODO: producer...


    public KafkaClient_0_8_Module() {
        super("kafka");
    }

    @Provides
    @Singleton
    ConsumerFactory provideConsumerFactory(KafkaClient_0_8_Factory factory) {
        return factory.createConsumerFactory();
    }

    // make factory itself injectable to avoid parsing config twice for producer and consumer
    @Provides
    @Singleton
    KafkaClient_0_8_Factory provideFactory(ConfigurationFactory configurationFactory) {
        return configurationFactory.config(KafkaClient_0_8_Factory.class, configPrefix);
    }
}
