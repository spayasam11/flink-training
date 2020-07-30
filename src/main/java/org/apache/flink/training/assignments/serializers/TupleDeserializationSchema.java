package org.apache.flink.training.assignments.serializers;

import akka.japi.tuple.Tuple4;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.training.assignments.domain.Order;

import java.io.IOException;

public class TupleDeserializationSchema implements DeserializationSchema<Tuple4<String, String, String, Integer>> {
    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    @Override
    public Tuple4<String, String, String, Integer> deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Tuple4.class);
    }
    @Override
    public boolean isEndOfStream(Tuple4<String, String, String, Integer> inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<Tuple4<String, String, String, Integer>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple4<String, String,String,Integer>>(){});
    }
}