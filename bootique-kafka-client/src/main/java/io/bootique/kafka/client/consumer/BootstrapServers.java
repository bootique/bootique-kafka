package io.bootique.kafka.client.consumer;

import java.util.Collection;
import java.util.Objects;

/**
 * @since 0.2
 */
public class BootstrapServers {

    private String bootstrapServers;

    public BootstrapServers(String bootstrapServers) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
    }

    public BootstrapServers(Collection<String> bootstrapServers) {
        if (Objects.requireNonNull(bootstrapServers).isEmpty()) {
            throw new IllegalArgumentException("Emoty list of bootstrap servers");
        }

        this.bootstrapServers = String.join(",", bootstrapServers);
    }

    public String asString() {
        return bootstrapServers;
    }
}
