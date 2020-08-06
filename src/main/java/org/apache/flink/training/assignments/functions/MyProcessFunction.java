package org.apache.flink.training.assignments.functions;

import akka.japi.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Hashtable;

public class MyProcessFunction
        extends ProcessWindowFunction<Tuple4<String, String,String,Integer>, Tuple4<String, String,String,Integer>, String,TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple4<String, String,String,Integer>> input, Collector<Tuple4<String, String,String,Integer>> out) {
        //long count = 0;
        System.out.println("Debug process");
        Hashtable uniqueByAcctSubCusip = new Hashtable<String,Tuple4<String,String,String,Integer>>();
        for (Tuple4<String, String,String,Integer> in: input) {
            if(uniqueByAcctSubCusip.containsKey((in.t1()+in.t2()+in.t3()).toString() )) {
                Tuple4<String, String,String,Integer> tempTuple = (Tuple4<String, String,String,Integer>)
                                                            uniqueByAcctSubCusip.get(in.t1()+in.t2()+in.t3());
                Tuple4<String, String,String,Integer> newTuple = new Tuple4<String, String,String,Integer>(in.t1(),in.t2(),in.t3(),tempTuple.t4() + in.t4());
                uniqueByAcctSubCusip.replace(in.t1()+in.t2()+in.t3(),newTuple);
            }
            else
            {
                uniqueByAcctSubCusip.put((in.t1()+in.t2()+in.t3()).toString(),in);
            }
        }
        uniqueByAcctSubCusip.forEach((ky, val) -> {
            out.collect((Tuple4<String,String,String,Integer>)val);
        });

    }
}