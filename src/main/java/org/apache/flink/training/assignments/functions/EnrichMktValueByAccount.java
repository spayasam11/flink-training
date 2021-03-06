package org.apache.flink.training.assignments.functions;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.FlatCompositeOrder;
import org.apache.flink.training.assignments.domain.FlatSymbolOrder;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class EnrichMktValueByAccount extends RichCoFlatMapFunction<Price, FlatCompositeOrder, Position> {

    ValueState<Price> prices;
    ValueState<FlatCompositeOrder> cusips;
    //Declare Guage

    @Override
    public void open(Configuration config) {
        prices = getRuntimeContext().getState(new ValueStateDescriptor<>("Price State", Price.class));
        cusips = getRuntimeContext().getState(new ValueStateDescriptor<>("Cusip State", FlatCompositeOrder.class));
    }

    @Override
    public void flatMap1(Price price, Collector<Position> out) throws Exception {
        FlatCompositeOrder fso = cusips.value();
        if(fso != null) {
            out.collect(new Position(price.getCusip(),fso.getAccount(),fso.getSubAccount(),fso.getQuantity(),
                    price.getPrice().multiply(new BigDecimal(fso.getQuantity())),0));
        } else {
            prices.update(price);
        }
    }

    @Override
    public void flatMap2(FlatCompositeOrder cusip, Collector<Position> out) throws Exception {
        Price price = prices.value();
        if (price != null ) {
            out.collect(new Position(cusip.getCusip(),cusip.getAccount(),cusip.getSubAccount(),cusip.getQuantity(),
                    price.getPrice().multiply(new BigDecimal(cusip.getQuantity())),0));
        }else {
            cusips.update(cusip);
        }// get same price again can be ignored.
    }
}
