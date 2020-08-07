package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Price;

public class PriceSerializationSchema
        implements SerializationSchema<Price> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Price element) {
        byte[] bytes = new byte[0];
        try {
            return (objectMapper.writeValueAsString(element).getBytes());
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        return bytes;
    }
}