package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.util.Collector;

public class MyGuage extends RichFlatMapFunction<Order, FlatOrder> {
    private transient long currentTimeLapsed = 0;

    @Override
    public void open(Configuration config) {
        currentTimeLapsed = System.currentTimeMillis();

        getRuntimeContext()
                .getMetricGroup()
                .gauge("OrderGauge", new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return currentTimeLapsed;
                    }
                });
    }


    @Override
    public void flatMap(Order value, Collector<FlatOrder> out) throws Exception {
        long currentTime = System.currentTimeMillis();
        for (Allocation allocation : value.getAllocations()) {
            out.collect(new FlatOrder(
                    value.getCusip(), (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1),
                    allocation.getAccount(), // consider Sell accounts as negative qty
                    allocation.getSubAccount()
            ));
        }
        // get Time lapsed to measure the guage.
        currentTimeLapsed = System.currentTimeMillis()-currentTimeLapsed;
    }
}