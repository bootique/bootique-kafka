package io.bootique.kafka.client;

import io.bootique.annotation.BQConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * @since 0.2
 */
@BQConfig
public class BootstrapServers {

    private String bootstrapServers;

    // TODO: allow @BQConfigProperty to be used on constructors
    // https://github.com/bootique/bootique/issues/127
    public BootstrapServers(String bootstrapServers) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
    }

    // TODO: allow @BQConfigProperty to be used on constructors
    // https://github.com/bootique/bootique/issues/127
    public BootstrapServers(Collection<String> bootstrapServers) {
        if (Objects.requireNonNull(bootstrapServers).isEmpty()) {
            throw new IllegalArgumentException("Empty list of bootstrap servers");
        }

        this.bootstrapServers = String.join(",", bootstrapServers);
    }

    public String asString() {
        return bootstrapServers;
    }
}
