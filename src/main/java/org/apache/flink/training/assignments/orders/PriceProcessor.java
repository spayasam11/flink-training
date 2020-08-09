package org.apache.flink.training.assignments.orders;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.*;
import org.apache.flink.training.assignments.serializers.*;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.training.assignments.utils.PropReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import org.apache.flink.training.assignments.serializers.FlatSymbolOrderDeserializationSchema;
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

public class PriceProcessor extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(PriceProcessor.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.srini-jenkins.wsn.riskfocus.com:9092";
    public static final String KAFKA_GROUP = "";
    public static final String IN_TOPIC_1 = "price";
    public static final String IN_TOPIC_2 = "positionsBySymbol";
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

        // Task 3 : Subscribe to Price topic
        FlinkKafkaConsumer010<Price> consumer1 = new FlinkKafkaConsumer010<Price>(IN_TOPIC_1,
                new PriceDeserializationSchema()
                , props);

        DataStream<Price> flatmapPrices = env.addSource(consumer1).keyBy(price -> price.getCusip()).assignTimestampsAndWatermarks(new BoundedOutOfPriceGenerator());
        printOrTest(flatmapPrices);

        // Task 3 : Subscribe to FlatOrder topic
        FlinkKafkaConsumer010<FlatSymbolOrder> consumer2 = new FlinkKafkaConsumer010<FlatSymbolOrder>(IN_TOPIC_2,
                new FlatSymbolOrderDeserializationSchema()
                , props);

        DataStream<FlatSymbolOrder> flatSymbolPrices = env.addSource(consumer2).keyBy(flatOrder -> flatOrder.getCusip());
                //.assignTimestampsAndWatermarks(new BoundedOutOfPriceGenerator());
        printOrTest(flatSymbolPrices);

        DataStream<Position> pos =  flatmapPrices.keyBy(pr -> pr.getCusip())
        .connect(flatSymbolPrices.keyBy(fsp -> fsp.getCusip()))
        .flatMap(new EnrichMktValue());
        printOrTest(pos);

        env.execute("kafkaOrders for Srini Assignment- task#2 namespace");
    }

}
