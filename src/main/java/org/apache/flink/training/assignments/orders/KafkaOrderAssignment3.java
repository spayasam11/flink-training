package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.BoundedOutOfOrdernessGenerator;
import org.apache.flink.training.assignments.functions.BoundedOutOfPriceGenerator;
import org.apache.flink.training.assignments.functions.GroupByCusip;
import org.apache.flink.training.assignments.functions.GroupByKey;
import org.apache.flink.training.assignments.serializers.FlatOrderSerializationSchema;
import org.apache.flink.training.assignments.serializers.FlatSymbolOrderSerializationSchema;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
import org.apache.flink.training.assignments.serializers.PriceDeserializationSchema;
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

public class KafkaOrderAssignment3 extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment3.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini-jenkins.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "positionsByAct";
    public static final String OUT_TOPIC_1 = "positionsBySymbol";
    public static final String KAFKA_GROUP = "";
    public static final String IN_TOPIC_1 = "price";
    public static Properties props = new Properties();

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set to 1 for now
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        FlinkKafkaProducer010<FlatOrder> producer =
                new FlinkKafkaProducer010<FlatOrder>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC,
                                new FlatOrderSerializationSchema());
        producer.setWriteTimestampToKafka(true);

        FlinkKafkaProducer010<FlatSymbolOrder> producer2 =
                new FlinkKafkaProducer010<FlatSymbolOrder>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC_1,
                                new FlatSymbolOrderSerializationSchema());
        producer2.setWriteTimestampToKafka(true);

        // Allocations are by Cusip, so keyBy Cusip.
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        DataStream<FlatOrder> flatmapStream = env.addSource(consumer).keyBy("cusip")
                .flatMap(new FlatMapFunction<Order,
                        FlatOrder>() {
                    @Override
                    public void flatMap(Order value, Collector<FlatOrder> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new FlatOrder(
                                    value.getCusip(), (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1),
                                    allocation.getSubAccount().getAccount(),
                                    allocation.getSubAccount().getAccount()
                            ));
                            // consider Sell accounts as negative qty
                        }
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        // Task # 2 : PositionBySymbol
       /* DataStream<FlatSymbolOrder> symbolStream = processStream
                .keyBy(flatSymbolOrder -> flatSymbolOrder.getCusip())
                .process(new GroupByCusip());
                */
        DataStream<FlatSymbolOrder> symbolStream = flatmapStream
                .keyBy(flatOrder -> flatOrder.getCusip())
                .process(new GroupByCusip());
        symbolStream.addSink(producer2);
        printOrTest(symbolStream);

        //printOrTest(flatmapStream);
        DataStream<FlatOrder> processStream =
                flatmapStream
                        .keyBy(flatOrder -> new CompositeKey(flatOrder.getCusip(), flatOrder.getAccount(), flatOrder.getSubAccount())) // Key is a combination of Cusip,Acct,Sub.
                        .process(new GroupByKey());

        //printOrTest(processStream);
        // publish it  to the out stream.
        processStream.addSink(producer);
        printOrTest(processStream);



        env.execute("kafkaOrders for Srini Assignment- task#1 namespace");
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
}
