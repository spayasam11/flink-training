package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.Order;

import java.io.IOException;

public class OrderDeserializationSchema implements DeserializationSchema<Order> {
    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    @Override
    public Order deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Order.class);
    }
    @Override
    public boolean isEndOfStream(Order inputMessage) {
        return false;
    }
    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}