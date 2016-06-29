package io.bootique.kafka.client_0_8;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.nhl.bootique.ConfigModule;
import com.nhl.bootique.config.ConfigurationFactory;
import com.nhl.bootique.jackson.JacksonService;
import io.bootique.kafka.client_0_8.consumer.ConsumerFactory;
import io.bootique.kafka.client_0_8.decoder.JsonDecoder;

public class KafkaClient_0_8_Module extends ConfigModule {

// TODO: producer...

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

    @Provides
    @Singleton
    JsonDecoder provideJsonDecoder(JacksonService jacksonService) {
        return new JsonDecoder(jacksonService.newObjectMapper());
    }
}
