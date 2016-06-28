package io.bootique.kafka.client_0_8;

import kafka.consumer.ConsumerConfig;

import java.util.Properties;

public class ConsumerConfigFactory {

    private String zookeeperConnect = "localhost:2181";
    private long zookeeperSessionTimeoutMs = 400;
    private long zookeeperSyncTimeMs = 200;
    private long autoCommitIntervalMs = 1000;
    private String group;


    public ConsumerConfig createConsumerConfig() {

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        
        return new ConsumerConfig(props);
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public void setZookeeperSessionTimeoutMs(long zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public void setZookeeperSyncTimeMs(long zookeeperSyncTimeMs) {
        this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
    }

    public void setAutoCommitIntervalMs(long autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
