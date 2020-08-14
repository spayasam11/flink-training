package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.BoundedOutOfOrdernessGenerator;
import org.apache.flink.training.assignments.serializers.FlatCompositeOrderSerializationSchema;
import org.apache.flink.training.assignments.serializers.FlatOrderSerializationSchema;
import org.apache.flink.training.assignments.serializers.FlatSymbolOrderSerializationSchema;
import org.apache.flink.training.assignments.serializers.OrderDeserializationSchema;
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

public class OrderProcessor extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(OrderProcessor.class);

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
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new PropReader().getProps();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);
        //env.enableCheckpointing(50000);

        // Extra credit: How would you use KafkaDeserializationSchema instead?
        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> consumer = new FlinkKafkaConsumer010<Order>(IN_TOPIC,
                new OrderDeserializationSchema()
                , props);
        // uid - Grafana , name - flink.
        DataStream<Order> orderStream = env.addSource(consumer).name("Subscribe Trades/Orders").uid("Subscribe_Trades_Orders");// .setParallelism(1)
        // printOrTest(orderStream);
        // create a Producer for positionsByAct
        FlinkKafkaProducer010<FlatOrder> producer =
                new FlinkKafkaProducer010<FlatOrder>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC,
                                new FlatOrderSerializationSchema());
        producer.setWriteTimestampToKafka(true);

        //// create a Producer for positionsBySymbol
        FlinkKafkaProducer010<FlatOrder> producer2 =
                new FlinkKafkaProducer010<FlatOrder>
                        (KAFKA_ADDRESS,
                                OUT_TOPIC_1,
                                new FlatOrderSerializationSchema());
        producer2.setWriteTimestampToKafka(true);

        // Allocations are by Cusip, so keyBy Cusip.
        // flatten the structure by extracting allocations for account, sub account,cusip and quantity .
        // Convert to RichFlatMap and add a guage maetric.
        DataStream<FlatOrder> flatmapStream = orderStream.keyBy(order -> order.getOrderId())
                .flatMap(new FlatMapFunction<Order,
                        FlatOrder>() {
                    @Override
                    public void flatMap(Order value, Collector<FlatOrder> out)
                            throws Exception {
                        long currentTime = System.currentTimeMillis();
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new FlatOrder(
                                    value.getCusip(), (value.getBuySell() == BuySell.BUY ? allocation.getQuantity() : allocation.getQuantity() * -1),
                                    allocation.getSubAccount().getAccount(), // consider Sell accounts as negative qty
                                    allocation.getSubAccount().getSubAccount()
                            ));
                        }
                        long currentTimeLapsed = System.currentTimeMillis();


                    }
                })//.setParallelism(4)
                .name("flatten the orders").uid("flatten the orders").assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                  .keyBy(flatOrder -> flatOrder.getCusip())
                  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                  .allowedLateness(Time.seconds(10))
                  .sum("quantity").name(" Aggregate Quantity By Cusip/Symbol").uid("Aggregate Quantity By Cusip/Symbol");
                    // .setParallelism(4)
                  //.process(new AddQty()).name(" Aggregate Quantity By Cusip/Symbol").uid("Aggregate Quantity By Cusip/Symbol");
        flatmapStream.addSink(producer2).name(" Publish to positionsBySymbol").uid("Publish to positionsBySymbol");// publish to positionsBySymbol.
        //printOrTest(flatmapStream);

        // Task # 2 : PositionByAccount
        DataStream<FlatOrder> flatCompositeMapStream = orderStream.keyBy(order -> order.getOrderId())
                .flatMap(new FlatMapFunction<Order,
                        FlatOrder>() {
                    @Override
                    public void flatMap(Order value, Collector<FlatOrder> out)
                            throws Exception {
                        for (Allocation allocation : value.getAllocations()) {
                            out.collect(new FlatOrder(
                                    value.getCusip(), (value.getBuySell() ==
                                                        BuySell.BUY ? allocation.getQuantity() :
                                                        allocation.getQuantity() * -1),
                                    allocation.getSubAccount().getAccount(),
                                    allocation.getSubAccount().getSubAccount()
                            ));
                            // consider Sell accounts as negative qty
                        }
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).name("Assign Watermarks").uid("Assign Watermarks")
                .keyBy(flatOrder -> new CompositeKey( flatOrder.getCusip(),flatOrder.getAccount(),flatOrder.getSubAccount()))
                .window(TumblingEventTimeWindows.of(Time.minutes(3)))
                .allowedLateness(Time.seconds(10))
                .sum("quantity").name("Aggregate Qty by Cusip/Acct/Subacct Composite key").uid("Aggregate Qty by Cusip/Acct/Subacct Composite key");
                //.process(new AddQtyByKey()).name(" Aggregate Qty by Cusip/Acct/Subacct Composite key").uid("Aggregate Qty by Cusip/Acct/Subacct Composite key");
            printOrTest(flatCompositeMapStream);
        flatCompositeMapStream.addSink(producer).name("Publish to positionsByAct").uid("Publish to positionsByAct");

        env.execute("Order Processor ");
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
     * Wraps the pre-aggregated result key By Cusip .
     */
    public static class AddQty extends ProcessWindowFunction<
                FlatOrder, FlatSymbolOrder, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<FlatOrder> orders, Collector<FlatSymbolOrder> out)
                throws Exception {
            int qty = 0;
            for (FlatOrder f : orders) {
                qty += f.getQuantity();
            }
            out.collect(new FlatSymbolOrder(key, qty));
        }
    }

    /*
     * Wraps the pre-aggregated result .
     */
    public static class AddQtyByKey extends ProcessWindowFunction<
            FlatOrder, FlatCompositeOrder, CompositeKey, TimeWindow> {
        @Override
        public void process(CompositeKey key, Context context, Iterable<FlatOrder> orders, Collector<FlatCompositeOrder> out)
                throws Exception {
            int qty = 0;
            for (FlatOrder f : orders) {
                qty += f.getQuantity();
            }
            out.collect(new FlatCompositeOrder(key.getCusip(),qty, key.getAccount(),key.getSubAccount()));
        }
    }

}
