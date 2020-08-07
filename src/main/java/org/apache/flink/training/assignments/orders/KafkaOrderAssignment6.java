package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
import org.apache.flink.training.assignments.serializers.Tuple3SerializationSchema;
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

public class KafkaOrderAssignment6 extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment6.class);

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
                , props);

        DataStream<Order> orderStream = env.addSource(consumer);
        printOrTest(orderStream);
        //orderStream.keyBy("cusip","account","subAccount").withTimestampAssigner((event, timestamp) -> event.f0);
        // create a Producer
        FlinkKafkaProducer010<Tuple3<String, Long, Integer>> producer =
                new FlinkKafkaProducer010<Tuple3<String, Long, Integer>>
                (KAFKA_ADDRESS,
                OUT_TOPIC,
                new Tuple3SerializationSchema());
        producer.setWriteTimestampToKafka(true);

        // Allocations are by Cusip, so keyBy Cusip.
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        DataStream<Tuple2<String,  Integer>> flatmapStream = env.addSource(consumer).keyBy("cusip","account","subAccount")
                .flatMap(new FlatMapFunction<Order,
                        Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(Order value, Collector<Tuple2<String,Integer>> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new Tuple2<String, Integer>(
                                    (value.getCusip() + "|" + allocation.getSubAccount() + "|" + allocation.getSubAccount()),
                                    (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1)));
                        }
                    }
                });

        //printOrTest(flatmapStream);
        // Why window by 5 mins, assuming we have an interval job evert 5 minutes to print running totals of held positions.
 //       TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(-100));
        DataStream<Tuple3<String, Long, Integer>> processStream =
                                flatmapStream
                                        .keyBy((Tuple2<String,  Integer> flatOrder) ->
                                               flatOrder.f0) // Key is a combination of Cusip|Acct|Sub.
                                        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                                        .process(new ProcessOrders());

        printOrTest(processStream);
        // publish it  to the out stream.
        processStream.addSink(producer);
        printOrTest(processStream);
        DataStream<Tuple3<String, Long, Integer>> qtyMax = processStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                .maxBy(2);
        printOrTest(qtyMax);
        env.execute("kafkaOrders for Srini Assignment6");
    // Add watermark.

    }
    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class ProcessOrders extends ProcessWindowFunction
            <Tuple2<String,Integer>, Tuple3<String, Long, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String,Integer>> orders, Collector<Tuple3<String, Long, Integer>> out) {
            int sumOfQty = 0;
            for (Tuple2<String,Integer> f : orders) {
                sumOfQty += f.f1;
            }
            out.collect(Tuple3.of(key,context.window().getEnd(),sumOfQty));
        }
    }
    }
