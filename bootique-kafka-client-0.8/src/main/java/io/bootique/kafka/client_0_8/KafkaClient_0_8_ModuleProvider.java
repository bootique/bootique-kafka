package io.bootique.kafka.client_0_8;


import com.google.inject.Module;
import io.bootique.BQModule;
import io.bootique.BQModuleProvider;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

public class KafkaClient_0_8_ModuleProvider implements BQModuleProvider {

    @Override
    public Module module() {
        return new KafkaClient_0_8_Module();
    }

    @Override
    public Map<String, Type> configs() {
        // TODO: config prefix is hardcoded. Refactor away from ConfigModule, and make provider
        // generate config prefix, reusing it in metadata...
        return Collections.singletonMap("kafkaclient", KafkaClient_0_8_Factory.class);
    }

    @Override
    public BQModule.Builder moduleBuilder() {
        return BQModuleProvider.super
                .moduleBuilder()
                .description("Provides integration with Apache Kafka client library, v.0.8");
    }
}
