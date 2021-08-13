package com.arctype;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class IdDeserializer implements Deserializer<String> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props, boolean isKey) { }

    @Override
    public void close() { }

    @Override
    public String deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        String id;
        try {
            Map payload = objectMapper.readValue(new String(bytes), Map.class);
            id = String.valueOf(payload.get("id"));
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return id;
    }
}
