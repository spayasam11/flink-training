package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.Price;

import java.io.IOException;

public class PriceDeserializationSchema implements DeserializationSchema<Price>  {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Price deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message,Price.class);
    }

    @Override
    public boolean isEndOfStream(Price nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Price> getProducedType()
    {
        return TypeInformation.of(Price.class);
    }
}


