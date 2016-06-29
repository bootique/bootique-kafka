package io.bootique.kafka.client_0_8;


import io.bootique.kafka.client_0_8.consumer.ConsumerConfigFactory;
import io.bootique.kafka.client_0_8.consumer.DefaultConsumerFactory;

import java.util.HashMap;
import java.util.Map;

// separating factory methods for producer and consumer ... only one may be needed in reality
// TODO: implement producers
public class KafkaClient_0_8_Factory {

    private Map<String, ConsumerConfigFactory> consumers;

    public DefaultConsumerFactory createConsumerFactory() {
        Map<String, Map<String, String>> configMap = new HashMap<>();

        if(consumers != null) {
            consumers.forEach((name, factory) -> configMap.put(name, factory.createConsumerConfig()));
        }

        return new DefaultConsumerFactory(configMap);
    }

    public void setConsumers(Map<String, ConsumerConfigFactory> consumers) {
        this.consumers = consumers;
    }
}
