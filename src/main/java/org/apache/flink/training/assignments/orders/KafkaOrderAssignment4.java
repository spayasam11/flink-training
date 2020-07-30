package org.apache.flink.training.assignments.orders;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
import org.apache.flink.training.assignments.serializers.TupleSerializationSchema;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.utils.PropReader;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*
Using the supplied order-generator.jar populate kafka topic
“in” with order data
Note: Must first edit application.properties and replace bootstrap.servers
with your kafka cluster URL: kafka.dest.rlewis.wsn.riskfocus.com:9092
2. Write a Flink application to consume this data
3. Add timestamps and watermarks to this data
4. Group the data by account
5. Write the data to Kafka topic “demo-output”
 */

public class KafkaOrderAssignment4 extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment4.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "demo-output";
    public static final String KAFKA_GROUP = "";
    public static Properties props = new Properties();

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Set to 1 for now
        env.setParallelism(1);
        Properties props = new PropReader().getProps();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        // Extra credit: How would you use KafkaDeserializationSchema instead?
        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> consumer = new FlinkKafkaConsumer010<Order>(IN_TOPIC,
                new OrderDeserializationSchema()
                , props);

        DataStream<Order> orderStream = env.addSource(consumer);
        printOrTest(orderStream);
        // create a Producer
        FlinkKafkaProducer010<Tuple4<String, String, String, Integer>> producer =
                new FlinkKafkaProducer010<Tuple4<String, String, String, Integer>>
                (KAFKA_ADDRESS,
                OUT_TOPIC,
                new TupleSerializationSchema());
        producer.setWriteTimestampToKafka(true);

        // Allocations are by Cusip, so keyBy Cusip.
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        DataStream<Tuple4<String, String, String, Integer>> flatmapStream = env.addSource(consumer).keyBy("cusip")
                .flatMap(new FlatMapFunction<Order,
                        Tuple4<String, String, String, Integer>>() {
                    @Override
                    public void flatMap(Order value, Collector<Tuple4<String, String, String, Integer>> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new Tuple4<String, String, String, Integer>(
                                    value.getCusip(),
                                    allocation.getAccount(),
                                    allocation.getSubAccount(),
                                    (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1)));
                                    // consider Sell accounts as negative qty
                        }
                    }
                });

        printOrTest(flatmapStream);
        // Why window by 1 hour, assuming we have an hourly job to print running totals of held positions.
        DataStream<Tuple4<String, String,String, Integer>> processStream =
                                flatmapStream
                                        .keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
                                            @Override
                                            public String getKey(Tuple4<String, String, String, Integer> compositeKey)
                                                    throws Exception {
                                                return compositeKey.t1()+compositeKey.t2()+compositeKey.t3();
                                            }
                                        });
                                        /*.reduceGroup(new GroupReduceFunction<Tuple4<String, String, String, Integer>, String>,
                                                Tuple4<String, String, String, Integer>, String>>(){
                                              @Override
                                                public void reduce(Iterable<Tuple4<String, String, String, Integer>, String>> in, Collector<Tuple4<String, String, String, Integer>, String>> out) {

                                                    Set<String> uniqStrings = new HashSet<String>();
                                                    Integer key = null;

                                                    // add all strings of the group to the set
                                                    for (Tuple4<String, String, String, Integer> t : in) {
                                                        key = t.f0;
                                                        uniqStrings.add(t.f1);
                                                    }

                                                    // emit all unique strings.
                                                    for (String s : uniqStrings) {
                                                        out.collect(new Tuple4<String, String, String, Integer>(key, s));
                                                    }
                                                }
                                          }
                                )




                                        ) // Key is a combination of Cusip,Acct,Sub.
                                        .process(new GroupByKey());
    */

        // publish it  to the out stream.
        processStream.addSink(producer);

        //env.execute("kafkaOrders for Srini Assignment3 namespace");


    }
    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {

        public String account;
        public int qty;
        public String subaccount;
        public String cusip;
        public long  lastModified;
    }


    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class GroupByKey extends KeyedProcessFunction<String,
                     Tuple4<String,String,String,Integer>,Tuple4<String,String,String,Integer>> {
        /** The state that is maintained by this process function */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration config) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(Tuple4<String, String, String, Integer> value, Context context,
                                   Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.cusip = value.t1();
                current.account = value.t2();
                current.subaccount = value.t3();
            }
            // Qty gets added ..
            current.qty += value.t4();

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = context.timestamp();

            // write the state back
            state.update(current);
            // how do we know the stream has ended to return the rolling sum.
            out.collect(new Tuple4<>(current.cusip,current.account,current.subaccount,current.qty));
            // why do we no timer needed for this implementation.
            //context.timerService().registerEventTimeTimer(current.lastModified + 60000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple4<String, String, String, Integer>> out)
                throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 60000) {
                // emit the state on timeout
                out.collect(new Tuple4<String, String,String, Integer>(result.cusip,result.account,result.subaccount,result.qty));
            }
        }


            //out.collect(new Tuple4<>(context.f.t1(),f.t2(),f.t3(),sumOfQty));

        }
    }
