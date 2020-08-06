package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.FlatSymbolOrder;

public class FlatSymbolOrderSerializationSchema
        implements SerializationSchema<FlatSymbolOrder> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(FlatSymbolOrder element) {
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