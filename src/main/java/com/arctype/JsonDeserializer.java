package com.arctype;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class JsonDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> clazz;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        clazz = (Class<T>) props.get("Class");
    }

    @Override
    public void close() { }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        Map payload;
        try {
            payload = objectMapper.readValue(new String(bytes), Map.class);
            // Debezium updates will contain a key "after" with the latest row contents.
            Map afterMap = (Map) payload.get("after");
            if (afterMap == null) {
                 // Non-Debezium payloads
                data = objectMapper.readValue(objectMapper.writeValueAsBytes(payload), clazz);
            } else {
                 // Incoming from Debezium
                data = objectMapper.readValue(objectMapper.writeValueAsBytes(afterMap), clazz);
            }

        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return data;
    }
}