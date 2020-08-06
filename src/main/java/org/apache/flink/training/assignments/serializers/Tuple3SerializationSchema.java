package org.apache.flink.training.assignments.serializers;

import akka.japi.tuple.Tuple4;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;

public class Tuple3SerializationSchema
        implements SerializationSchema<Tuple3<String, Long, Integer>> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Tuple3<String, Long, Integer> element) {
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