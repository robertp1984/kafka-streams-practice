package org.softwarecave.org.softwarecave.kafkastreamscommon;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    public JsonSerde(Class<T> clazz) {
        ObjectMapper mapper = new ObjectMapper();

        this.serializer = new JsonSerializer<>(mapper);
        this.deserializer = new JsonDeserializer<>(mapper, clazz);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
