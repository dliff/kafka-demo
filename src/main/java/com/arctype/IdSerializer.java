package com.arctype;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class IdSerializer implements Serializer<String> {
    @Override
    public void configure(Map<String, ?> props, boolean isKey) { }

    @Override
    public void close() { }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null)
            return null;

        return data.getBytes();
    }
}
