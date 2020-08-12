package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.FlatCompositeOrder;
import org.apache.flink.training.assignments.domain.FlatOrder;

import java.io.IOException;

public class FlatCompositeOrderDeserializationSchema implements DeserializationSchema<FlatCompositeOrder>  {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public FlatCompositeOrder deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message,FlatCompositeOrder.class);
    }

    @Override
    public boolean isEndOfStream(FlatCompositeOrder nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlatCompositeOrder> getProducedType()
    {
        return TypeInformation.of(FlatCompositeOrder.class);
    }
}


