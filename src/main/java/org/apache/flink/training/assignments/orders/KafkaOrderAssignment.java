package org.apache.flink.training.assignments.orders;

import akka.japi.tuple.Tuple4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

public class KafkaOrderAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini-jenkins.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "positionsByAct";//positionsByAct";//demo-output
    public static final String KAFKA_GROUP = "1";
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

        // key by Cusip
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
                }).name("flatmap by Allocation-AccountName/SubAccount/Cusip").uid("flatmap by Allocation-AccountName/SubAccount/Cusip");
        //flatmapStream.addSink(producer).name("Publish to "+OUT_TOPIC).uid("Publish to "+OUT_TOPIC);
        // filter exercise only for logging.
                    ;

        /*printOrTest(flatmapStream.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Integer> in) throws Exception {
                return in.t2().equals("ACC16") && in.t3().equals("S18");
            }
        }));
        */
        //System.out.print("flatmapStream");
        printOrTest(flatmapStream);
        // Why window by 1 hour, assuming we have an hourly job to print running totals of held positions.
        /*DataStream<Tuple4<String, String, String, Integer>> aggregateStream =
                        flatmapStream.keyBy(flatOrders -> flatOrders.t1()+flatOrders.t2()+flatOrders.t3())
                        .timeWindowAll(Time.minutes(5))
                        .allowedLateness(Time.seconds(30))
                        .aggregate( new MyAggregator())
                        .name("aggregate by AccountName / SubAccount name")
                        .uid("aggregate by AccountName / SubAccount name");
           */
        // publish it  to the out stream.
        //aggregateStream.addSink(producer).name("Publish to "+OUT_TOPIC).uid("Publish to "+OUT_TOPIC);



        // Consume it from the out stream to print running totals.
        /*FlinkKafkaConsumer010<Tuple4<String,String,String,Integer>> consumer2 = new FlinkKafkaConsumer010<Tuple4<String,String,String,Integer>>(OUT_TOPIC,
                new TupleDeserializationSchema()
                , props);

        DataStream<Tuple4<String,String,String,Integer>> readBackOrdersGrpBy = env.addSource(consumer2);
        */
        //System.out.print("readBackOrdersGrpBy");
        //printOrTest(readBackOrdersGrpBy);
        env.execute("kafkaOrders for Srini Assignment1");
    }
}
/*
{"timestamp":1596397336614,"eos":false,"orderId":"1596397336614-1404","cusip":"Cusip1","assetType":"Bond","buySell":"BUY","bidOffer":null,"currency":null,"quantity":285,"orderTime":0,"allocations":[{"subAccount":{"account":"ACC202","subAccount":"S3","benchmark":null,"composedKey":"ACC202_S3"},"quantity":1},{"subAccount":{"account":"ACC107","subAccount":"S8","benchmark":null,"composedKey":"ACC107_S8"},"quantity":1},{"subAccount":{"account":"ACC173","subAccount":"S7","benchmark":null,"composedKey""[truncated 30937 bytes]; line: 1, column: 39] (through reference chain: org.apache.flink.training.assignments.domain.Order["eos"])
{"timestamp":1596397340303,"eos":false,"orderId":"1596397340303-1779","cusip":"Cusip18","assetType":"Bond","buySell":"BUY","bidOffer":null,"currency":null,
"quantity":285,"orderTime":0,"allocations":[{"subAccount":{"account":"ACC202","subAccount":"S3","benchmark":null,"composedKey":"ACC202_S3"},"quantity":1},
{"subAccount":{"account":"ACC107","subAccount":"S8","benchmark":null,"composedKey":"ACC107_S8"},"quantity":1},
{"subAccount":{"account":"ACC173","subAccount":"S7","benchmark":null,"composedKey"[truncated 30938 bytes]; line: 1, column: 214]
(through reference chain: org.apache.flink.training.assignments.domain.Order["allocations"]->java.util.ArrayList[0]
->org.apache.flink.training.assignments.domain.Allocation["subAccount"])

 */