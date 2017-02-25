package io.bootique.kafka.client_0_8.consumer;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;

import java.util.Map;
import java.util.Objects;

@BQConfig
public class ConsumerConfigFactory extends ConsumerConfig {

    private String zookeeperConnect = "localhost:2181";


    public Map<String, String> createConsumerConfig() {

        Map<String, String> props = super.createConsumerConfig();
        props.put("zookeeper.connect", Objects.requireNonNull(zookeeperConnect));
        return props;
    }

    @BQConfigProperty
    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }
}
