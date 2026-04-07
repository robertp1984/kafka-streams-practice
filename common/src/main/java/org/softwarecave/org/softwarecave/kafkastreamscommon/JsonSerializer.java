package org.softwarecave.org.softwarecave.kafkastreamscommon;

import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper;

    public JsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return objectMapper.writeValueAsBytes(data);
    }
}
