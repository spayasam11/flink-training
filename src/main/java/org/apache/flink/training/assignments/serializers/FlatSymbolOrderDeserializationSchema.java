package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.FlatSymbolOrder;

import java.io.IOException;

public class FlatSymbolOrderDeserializationSchema implements DeserializationSchema<FlatSymbolOrder>  {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public FlatSymbolOrder deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message,FlatSymbolOrder.class);
    }

    @Override
    public boolean isEndOfStream(FlatSymbolOrder nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlatSymbolOrder> getProducedType()
    {
        return TypeInformation.of(FlatSymbolOrder.class);
    }
}


