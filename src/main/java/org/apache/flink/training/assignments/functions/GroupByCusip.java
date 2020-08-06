package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.assignments.domain.FlatOrder;
import org.apache.flink.training.assignments.domain.FlatSymbolOrder;
import org.apache.flink.training.assignments.orders.KafkaOrderAssignment3;
import org.apache.flink.util.Collector;

/*
 * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
 */
public class GroupByCusip extends KeyedProcessFunction<String,
        FlatOrder,FlatSymbolOrder> {
    /** The state that is maintained by this process function */
    private ValueState<KafkaOrderAssignment3.CountWithTimestamp> state;

    @Override
    public void open(Configuration config) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", KafkaOrderAssignment3.CountWithTimestamp.class));
    }

    @Override
    public void processElement(FlatOrder value, Context context,
                               Collector<FlatSymbolOrder> out) throws Exception {
        //System.out.println("process element");
        // retrieve the current count
        KafkaOrderAssignment3.CountWithTimestamp current = state.value();
        if (current == null) {
            current = new KafkaOrderAssignment3.CountWithTimestamp();
            current.cusip = value.getCusip();
        }

        // Qty gets added ..
        current.qty += value.getQuantity();
        //System.out.println("context.timestamp()"+context.timestamp());
        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = context.timestamp();
        //System.out.println("current.lastModified"+current.lastModified);

        // write the state back
        state.update(current);
        // how do we know the stream has ended to return the rolling sum.
        //out.collect(new Tuple4<>(current.cusip,current.account,current.subaccount,current.qty));
        // why do we no timer needed for this implementation.
        context.timerService().registerEventTimeTimer(current.lastModified + 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<FlatSymbolOrder> out)
            throws Exception {
        //System.out.println("onTimer"+timestamp);

        // get the state for the key that scheduled the timer
        KafkaOrderAssignment3.CountWithTimestamp result = state.value();
        //System.out.println("result.lastModified + 6000"+result.lastModified + 5000);
        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 1000) {
            // emit the state on timeout
            out.collect(new FlatSymbolOrder(result.cusip,result.qty));
        }
    }
}
