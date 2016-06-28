package io.bootique.kafka.client_0_8;


import com.google.inject.Module;
import com.nhl.bootique.BQModuleProvider;

public class KafkaClient_0_8_ModuleProvider implements BQModuleProvider {

    @Override
    public Module module() {
        return new KafkaClient_0_8_Module();
    }
}
