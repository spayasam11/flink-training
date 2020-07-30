package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.functions.AssignmentGroupBy;
import org.apache.flink.training.assignments.functions.MyWaterMarkAssigner;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
import org.apache.flink.training.assignments.serializers.OrderSerializationSchema;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.utils.PropReader;
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

public class KafkaOrderAssignmentTrial extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentTrial.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini1.wsn.riskfocus.com:9092";
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



        DataStream<Order> withTimestampsAndWatermarks = orderStream
                .rebalance()
                .keyBy("cusip")
                .assignTimestampsAndWatermarks(new MyWaterMarkAssigner());

        withTimestampsAndWatermarks.print();

        // create a Producer
        FlinkKafkaProducer010<Order> producer = new FlinkKafkaProducer010<>(KAFKA_ADDRESS,
                OUT_TOPIC,
                new OrderSerializationSchema());
        producer.setWriteTimestampToKafka(true);
                //collect all events in a one hour window
                withTimestampsAndWatermarks
                .keyBy("cusip")
                .timeWindow(Time.hours(1));
                //.aggregate(new AssignmentGroupBy());
                //.addSink(producer);

        // execute the transformation pipeline
        //env.execute("kafkaOrders-for-Srini trial namespace");
    }

}
