package org.apache.flink.training.assignments.serializers;

import akka.japi.tuple.Tuple4;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.Order;

public class TupleSerializationSchema
        implements SerializationSchema<Tuple4<String, String, String, Integer>> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Tuple4<String, String, String, Integer> element) {
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