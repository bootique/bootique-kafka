package io.bootique.kafka.client.producer;

import io.bootique.annotation.BQConfig;
import io.bootique.annotation.BQConfigProperty;
import io.bootique.kafka.client.BootstrapServers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.bootique.kafka.client.FactoryUtils.setProperty;
import static io.bootique.kafka.client.FactoryUtils.setRequiredProperty;

/**
 * @since 0.2
 */
@BQConfig
public class ProducerFactory {

    private static final String BOOTSTRAP_SERVERS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String ACKS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
    private static final String RETRIES_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
    private static final String BATCH_SIZE_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
    private static final String LINGER_MS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
    private static final String BUFFER_MEMORY_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;


    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);

    private String acks;
    private int retries;
    private int batchSize;
    private int lingerMs;
    private int bufferMemory;

    public ProducerFactory() {

        this.acks = "all";
        this.retries = 0;
        this.batchSize = 16384;
        this.lingerMs = 1;
        this.bufferMemory = 33554432;
    }

    @BQConfigProperty
    public void setAcks(String acks) {
        this.acks = acks;
    }

    @BQConfigProperty
    public void setRetries(int retries) {
        this.retries = retries;
    }

    @BQConfigProperty
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @BQConfigProperty
    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    @BQConfigProperty
    public void setBufferMemory(int bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public <K, V> Producer<K, V> createProducer(BootstrapServers bootstrapServers, ProducerConfig<K, V> config) {

        Map<String, Object> properties = new HashMap<>();

        setRequiredProperty(properties,
                BOOTSTRAP_SERVERS_CONFIG,
                Objects.requireNonNull(bootstrapServers).asString());

        setProperty(properties,
                ACKS_CONFIG,
                config.getAcks(),
                acks);
        setProperty(properties,
                RETRIES_CONFIG,
                config.getRetries(),
                retries);
        setProperty(properties,
                BATCH_SIZE_CONFIG,
                config.getBatchSize(),
                batchSize);
        setProperty(properties,
                LINGER_MS_CONFIG,
                config.getLingerMs(),
                lingerMs);
        setProperty(properties,
                BUFFER_MEMORY_CONFIG,
                config.getBufferMemory(),
                bufferMemory);


        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Creating producer bootstrapping with %s.",
                    properties.get(BOOTSTRAP_SERVERS_CONFIG)));
        }

        return new KafkaProducer<>(properties, config.getKeySerializer(), config.getValueSerializer());
    }

}
