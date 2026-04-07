package org.softwarecave.org.softwarecave.kafkastreamscommon;

import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> clazz;

    public JsonDeserializer(ObjectMapper objectMapper, Class<T> clazz) {
        this.objectMapper = objectMapper;
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return objectMapper.readValue(data, clazz);
    }
}
