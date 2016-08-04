package io.bootique.kafka.client.producer;

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
public class ProducerFactory {

    private static final String BOOTSTRAP_SERVERS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    private static final String ACKS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
    private static final String RETRIES_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
    private static final String BATCH_SIZE_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
    private static final String LINGER_MS_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
    private static final String BUFFER_MEMORY_CONFIG = org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;


    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);

    private int acks;
    private int retries;
    private int batchSize;
    private int lingerMs;
    private int bufferMemory;

    public ProducerFactory() {

        // note that -1 is the default meaning "all"; 0 means no acks and is frequently not a good idea
        this.acks = -1;

        this.retries = 0;
        this.batchSize = 16384;
        this.lingerMs = 1;
        this.bufferMemory = 33554432;
    }

    public void setAcks(int acks) {
        this.acks = acks;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

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
