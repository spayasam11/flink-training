package org.apache.flink.training.assignments.orders;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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

import javax.annotation.Nullable;
import java.time.ZoneId;
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

public class KafkaOrderAssignment2 extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment2.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini-jenkins.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "positionsByAct";
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
                , props);//.assignTimestampsAndWatermarks(new InputMessageTimestampAssigner());

        DataStream<Order> orderStream = env.addSource(consumer);
        //printOrTest(orderStream);
        // create a Producer
        FlinkKafkaProducer010<Tuple4<String, String, String, Integer>> producer =
                new FlinkKafkaProducer010<Tuple4<String, String, String, Integer>>
                (KAFKA_ADDRESS,
                OUT_TOPIC,
                new TupleSerializationSchema());
        producer.setWriteTimestampToKafka(true);
        //producer.new InputMessageTimestampAssigner()

        // Why should we key by Cusip
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
                                    // consider Sell accounts as negative
                        }
                    }
                });
        printOrTest(flatmapStream);
        flatmapStream.addSink(producer);
       /*
        // filter non null accounts
        flatmapStream.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Integer> in) throws Exception {
                return in.t2() != null && in.t3()  != null && in.t1() != null;
            }
        });
        */
        //printOrTest(flatmapStream);
        // Why window by 1 hour, assuming we have an hourly job to print running totals of held positions.
        /*
        DataStream<Tuple4<String, String,String, Integer>> processStream =
                                flatmapStream
                                        .keyBy((Tuple4<String, String, String, Integer> flatOrder) -> flatOrder.t1()+flatOrder.t2()+flatOrder.t3())
                                        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                                        .allowedLateness(Time.seconds(10))
                                        .process(new GroupByAccount());



        printOrTest(processStream);
        // publish it  to the out stream.
        processStream.addSink(producer);
        */

        //env.execute("kafkaOrders for Srini Assignment2 namespace");


    }
    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class GroupByAccount extends ProcessWindowFunction<
                 Tuple4<String,String,String,Integer>,Tuple4<String,String,String,Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple4<String,String,String,Integer>> flatOrders,
                            Collector<Tuple4<String,String,String,Integer>> out) {
            int sumOfQty = 0;
            String cusip = "";
            String acct = "";
            String subacct = "";
            for (Tuple4<String,String,String,Integer> f : flatOrders) {
                cusip = f.t1();
                acct = f.t2();
                subacct = f.t3();
                sumOfQty += f.t4();
            }
            out.collect(new Tuple4<>(cusip,acct,subacct,sumOfQty));

            //out.collect(new Tuple4<>(context.f.t1(),f.t2(),f.t3(),sumOfQty));

        }
    }

    public static class InputMessageTimestampAssigner
            implements AssignerWithPunctuatedWatermarks<Order> {

        @Override
        public long extractTimestamp(Order order,
                                     long previousElementTimestamp) {
            ZoneId zoneId = ZoneId.systemDefault();
            return order.getTimestamp() * 1000;
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Order lastElement,
                                                  long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 1500);
        }
    }
}