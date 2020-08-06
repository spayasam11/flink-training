package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AggregateBySymbol extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<String, Integer>> sum;

    @Override
    public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {

        // access the state value
        Tuple2<String, Integer> currentSum = sum.value();

        // update the count
        currentSum.f0 += input.f0;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 != input.f0) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1));
            sum.clear();
        }
        //}
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}), // type information
                        Tuple2.of("", 0)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}
