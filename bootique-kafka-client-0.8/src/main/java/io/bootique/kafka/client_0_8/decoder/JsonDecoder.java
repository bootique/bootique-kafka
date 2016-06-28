package io.bootique.kafka.client_0_8.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;

import java.io.IOException;

public class JsonDecoder implements Decoder<JsonNode> {

    private ObjectMapper mapper;

    public JsonDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public JsonNode fromBytes(byte[] bytes) {

        // TODO: explicit encoding..

        try {
            return mapper.readTree(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Error reading config data", e);
        }
    }
}
