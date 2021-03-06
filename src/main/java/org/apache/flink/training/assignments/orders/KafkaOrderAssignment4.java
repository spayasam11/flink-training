package org.apache.flink.training.assignments.orders;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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
import org.apache.flink.training.assignments.serializers.Tuple2SerializationSchema;
import org.apache.flink.training.assignments.serializers.TupleSerializationSchema;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.utils.PropReader;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

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
        //WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
        // Extra credit: How would you use KafkaDeserializationSchema instead?
        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> consumer = new FlinkKafkaConsumer010<Order>(IN_TOPIC,
                new OrderDeserializationSchema()
                , props);

        DataStream<Order> orderStream = env.addSource(consumer);
        printOrTest(orderStream);
        // create a Producer
        FlinkKafkaProducer010<Tuple2<String, Integer>> producer =
                new FlinkKafkaProducer010<Tuple2<String, Integer>>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC,
                                new Tuple2SerializationSchema());
        producer.setWriteTimestampToKafka(true);

        // Allocations are by Cusip, so keyBy Cusip.
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        DataStream<Tuple2<String, Integer>> flatmapStream = env.addSource(consumer).keyBy("cusip")
                .flatMap(new FlatMapFunction<Order,
                        Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Order value, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new Tuple2<String, Integer>(
                                    value.getCusip(),
                                    (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1)));
                            // consider Sell accounts as negative qty
                        }
                    }
                });

        printOrTest(flatmapStream);
        // Why window by 1 hour, assuming we have an hourly job to print running totals of held positions.
        /*DataStream<Tuple2<String, Integer>> processStream =
                flatmapStream
                        .keyBy(0).timeWindow(Time.minutes(10))
                        .allowedLateness(Time.seconds(1))
                        .process(new GroupByKey());

        processStream.addSink(producer);

        printOrTest(processStream);
         */
        //env.execute("kafkaOrders for Srini Assignment3 namespace");


    }
    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {
        public String cusip;
        public int qty;
    }


    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class GroupByKey extends ProcessWindowFunction<
            Tuple2<String, Integer>, Tuple2<String, Integer>,String, TimeWindow> {
        /** The state that is maintained by this process function */
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) {
            int sum = 0;
            String cusip = "";
            for (Tuple2<String, Integer> in: input) {
                sum += in.f1;
                cusip = in.f0;
            }
            out.collect(new Tuple2<String, Integer>(cusip,sum));
        }
    }

}