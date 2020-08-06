package org.apache.flink.training.assignments.functions;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MySymbolAggregator
        implements AggregateFunction<Tuple2<String,Integer>,
                    Tuple2<String, Integer>,Tuple2<String,Integer>>
{

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<String,  Integer>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple2<String,Integer> value,
                                       Tuple2<String,Integer> accumulator) {
        return new Tuple2<String,Integer>(value.f0,accumulator.f1 + value.f1);
    }

    @Override
    public Tuple2<String,  Integer> getResult(Tuple2<String,  Integer> val) {
        return val;
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String,Integer> a, Tuple2<String, Integer> b) {
        return new Tuple2<String,Integer>(a.f0,a.f1 + b.f1);
    }
}
