package org.apache.flink.training.assignments.orders;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.*;
import org.apache.flink.training.assignments.serializers.*;
import org.apache.flink.training.assignments.sinks.LogSink;
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
    public static final String IN_TOPIC_3 = "positionsByAct";
    public static final String OUT_TOPIC_2 = "mvBySymbol";
    public static final String OUT_TOPIC_1 = "mvByAct";
    public static Properties props = new Properties();

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set to 1 for now
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new PropReader().getProps();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);
        //env.getConfig().setLatencyTrackingInterval(1000);
        env.getConfig().setLatencyTrackingInterval(5L);
        //env.enableCheckpointing(50000);

        // Task 1 : Subscribe to Prices
        FlinkKafkaConsumer010<Price> consumer1 = new FlinkKafkaConsumer010<Price>(IN_TOPIC_1,
                new PriceDeserializationSchema()
                , props);

        DataStream<Price> flatmapPrices = env.addSource(consumer1).name("Subscribe Prices").uid("Subscribe Prices")
                                          .keyBy(price -> price.getCusip())
                                          .assignTimestampsAndWatermarks(new BoundedOutOfPriceGenerator())
                                          .name("Assign Watermarkes on recvd Prices .").uid("Assign_Watermarks_On_Recvd_Prices .");
        printOrTest(flatmapPrices);

        // Task 2 : Subscribe to FlatOrder topic
        FlinkKafkaConsumer010<FlatOrder> consumer2 = new FlinkKafkaConsumer010<FlatOrder>(IN_TOPIC_2,
                new FlatOrderDeserializationSchema()
                , props);

        DataStream<FlatOrder> flatSymbolPrices = env.addSource(consumer2).keyBy(flatOrder -> flatOrder.getCusip());

        DataStream<Position> pos =  flatmapPrices.keyBy(pr -> pr.getCusip())
        .connect(flatSymbolPrices.keyBy(fsp -> fsp.getCusip()))
        .flatMap(new EnrichMktValue())
                .name("Enrich Mkt Value ").uid("Enrich_Mkt_Value");
        printOrTest(pos);

        // Create a Publisher for mktValueBySymbol.
        FlinkKafkaProducer010<Position> mvBySymbol =
                new FlinkKafkaProducer010<Position>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC_2,
                                new PositionSerializationSchema());
        mvBySymbol.setWriteTimestampToKafka(true);
        pos.addSink(mvBySymbol);

        //mvBySymbol.setLogFailuresOnly(true);
        //mvBySymbol.setFlushOnCheckpoint(true);
        // logging
        pos.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "PositionStream")).name("logging positions")
                .uid("logging_positions");

        // Task 3 : Subscribe to Composite Keys topic
        FlinkKafkaConsumer010<FlatOrder> consumer3 = new FlinkKafkaConsumer010<FlatOrder>(IN_TOPIC_3,
                new FlatOrderDeserializationSchema()
                , props);

        DataStream<FlatOrder> flatSymbolOrders = env.addSource(consumer3);

        DataStream<Position> posByAcct =flatmapPrices.keyBy(pr -> pr.getCusip())
                .connect(flatSymbolOrders
                .keyBy(fso -> fso.getCusip())).flatMap(new EnrichMktValue())
                .name("Enrich MktValue By Symbol").uid("Enrich MktValue By Symbol");

        // Create a Publisher for mktValueBySymbol.
        FlinkKafkaProducer010<Position> mvByAcct =
                new FlinkKafkaProducer010<Position>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC_1,
                                new PositionSerializationSchema());
        mvByAcct.setLogFailuresOnly(true);
        mvByAcct.setFlushOnCheckpoint(true);
        mvByAcct.setWriteTimestampToKafka(true);

        posByAcct.addSink(mvByAcct).name("Publish positions By Symbol").uid("Publish positions By Symbol");
        // logging
        posByAcct.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.DEBUG, "PositionByAccountStream"))
                .name(" Log Positions By Symbol").uid("Test");
        printOrTest(posByAcct);
        env.execute("Price Processor");
    }

}
