package org.apache.flink.training.assignments.functions;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.utils.MyOrders;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class MyAggregator
        implements AggregateFunction<Tuple4<String, String,String,Integer>, Tuple4<String, String,String,Integer>,Tuple4<String, String, String, Integer>>
{

    @Override
    public Tuple4<String, String,String,Integer> createAccumulator() {
        return new Tuple4<String, String, String, Integer>("", "","",0);
    }

    @Override
    public Tuple4<String, String,String,Integer>add(Tuple4<String, String,String,Integer> value,
                                                    Tuple4<String, String,String,Integer> accumulator) {
        return new Tuple4<>(accumulator.t1(),accumulator.t2(),accumulator.t3(),accumulator.t4() + value.t4());
    }

    @Override
    public Tuple4<String, String, String, Integer> getResult(Tuple4<String, String, String, Integer> val) {
        return val;
    }

    @Override
    public Tuple4<String, String,String,Integer> merge(Tuple4<String, String,String,Integer> a, Tuple4<String, String,String,Integer> b) {
        return new Tuple4<String, String,String,Integer>(a.t1(),a.t2(),a.t3(),a.t4() + b.t4());
    }
}
