package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.Order;

import java.io.IOException;

public class FlatOrderDeserializationSchema implements DeserializationSchema<FlatOrder>  {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public FlatOrder deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message,FlatOrder.class);
    }

    @Override
    public boolean isEndOfStream(FlatOrder nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlatOrder> getProducedType()
    {
        return TypeInformation.of(FlatOrder.class);
    }
}


