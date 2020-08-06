package org.apache.flink.training.assignments.orders;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.functions.AggregateBySymbol;
import org.apache.flink.training.assignments.functions.MyAggregator;
import org.apache.flink.training.assignments.functions.MySymbolAggregator;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
import org.apache.flink.training.assignments.serializers.Tuple2SerializationSchema;
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

public class KafkaOrderAssignmentJoinPrices extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentJoinPrices.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "positionsByAct";//demo-output
    public static final String OUT_TOPIC_SYMBOL = "positionsBySymbol";//demo-output
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
        //printOrTest(orderStream);
        // create a Producer
        FlinkKafkaProducer010<Tuple4<String, String, String, Integer>> producer =
                new FlinkKafkaProducer010<Tuple4<String, String, String, Integer>>
                (KAFKA_ADDRESS,
                OUT_TOPIC,
                new TupleSerializationSchema());
        producer.setWriteTimestampToKafka(true);

        // OUT_TOPIC_SYMBOL
        // create a Producer
        FlinkKafkaProducer010<Tuple2<String,Integer>> symbolProducer =
                new FlinkKafkaProducer010<Tuple2<String, Integer>>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC_SYMBOL,
                                new Tuple2SerializationSchema());
        symbolProducer.setWriteTimestampToKafka(true);


        // key by Cusip
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        DataStream<Tuple2<String,  Integer>> flatmapStream = env.addSource(consumer).keyBy("cusip")
                .flatMap(new FlatMapFunction<Order,
                        Tuple2<String,  Integer>>() {
                    @Override
                    public void flatMap(Order value, Collector<Tuple2<String,  Integer>> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new Tuple2<String,  Integer>(
                                    value.getCusip(),
                                    //allocation.getAccount(),
                                    //allocation.getSubAccount(),
                                    (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1)));
                                    // consider Sell accounts as negative
                        }
                    }
                }).name("flatmap by Allocation-AccountName/SubAccount/Cusip").uid("flatmap by Allocation-AccountName/SubAccount/Cusip");
                //.timeWindowAll(Time.minutes(3))
                //.sum(1);
        // filter exercise only for logging.
        /*printOrTest(flatmapStream.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Integer> in) throws Exception {
                return in.t2().equals("ACC16") && in.t3().equals("S18");
            }
        }));
        */
        System.out.print("flatmapStream");
        printOrTest(flatmapStream);
        // Why window by 1 hour, assuming we have an hourly job to print running totals of held positions.
        /*DataStream<Tuple4<String, String, String, Integer>> aggregateStream =
                        flatmapStream.keyBy(flatOrders -> flatOrders.t1()+flatOrders.t2()+flatOrders.t3())
                        .timeWindowAll(Time.minutes(1))
                        .allowedLateness(Time.minutes(1))
                        .aggregate( new MyAggregator())
                        .name("aggregate by AccountName / SubAccount name")
                        .uid("aggregate by AccountName / SubAccount name");
        printOrTest(flatmapStream);
        // publish it  to the out stream.
        aggregateStream.addSink(producer);
        */


        /* Aggregate by Symbol only
        DataStream<Tuple2<String, Integer>> aggregateBySymbolStream =
                flatmapStream.keyBy(flatOrders -> flatOrders.f0)
                        .timeWindowAll(Time.minutes(1))
                        .allowedLateness(Time.minutes(1))
                        .aggregate( new MySymbolAggregator())
                        .name("aggregate by Symbol/Cusip name")
                        .uid("aggregate by Symbol/Cusip name");
        printOrTest(aggregateBySymbolStream);

        aggregateBySymbolStream.addSink(symbolProducer);
        */
        // Consume it from the out stream to print running totals.
        /*FlinkKafkaConsumer010<Tuple4<String,String,String,Integer>> consumer2 = new FlinkKafkaConsumer010<Tuple4<String,String,String,Integer>>(OUT_TOPIC,
                new TupleDeserializationSchema()
                , props);

        DataStream<Tuple4<String,String,String,Integer>> readBackOrdersGrpBy = env.addSource(consumer2);
        */
        //System.out.print("readBackOrdersGrpBy");
        //printOrTest(readBackOrdersGrpBy);
        //env.execute("kafkaOrders for Join Prices with Orders");
    }
}