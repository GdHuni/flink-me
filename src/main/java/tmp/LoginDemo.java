package tmp;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LoginDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 1、数据源
         * 2、在数据源上做出watermark
         * 3、在watermark上根据id分组keyby
         * 4、做出模式pattern
         * 5、在数据流上进行模式匹配
         * 6、提取匹配成功的数据
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //2.读取数据源数据
        //设置kafka配置
/*        String topic = "huni_topic";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","mygp");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //设置从最新的offset开始消费
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);
        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);
         DataStream<LoginBean> data = env.addSource(consumer)
        */

        DataStream<LoginBean> data = env.socketTextStream("linux121",7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    LoginBean loginEvent = new LoginBean(new Long(fields[0]), fields[1],new Long(fields[2]));
                    return loginEvent;
                });


//        DataStreamSource<LoginBean> data = env.fromElements(
//                new LoginBean(1L, "fail", 1597905234000L),
//                new LoginBean(1L, "success", 1597905235000L),
//                new LoginBean(2L, "fail", 1597905236000L),
//                new LoginBean(2L, "fail", 1597905237000L),
//                new LoginBean(2L, "fail", 1597905238000L),
//                new LoginBean(3L, "fail", 1597905239000L),
//                new LoginBean(3L, "success", 1597905240000L)
//        );

        //2、在数据源上做出watermark
        SingleOutputStreamOperator<LoginBean> watermarks = data.assignTimestampsAndWatermarks(new WatermarkStrategy<LoginBean>() {
            @Override
            public WatermarkGenerator<LoginBean> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<LoginBean>() {
                    long maxTimeStamp = 0L;

                    @Override
                    public void onEvent(LoginBean event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTs());
                    }

                    long maxOutOfOrderness = 2000l;

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        //3、在watermark上根据id分组keyby
        KeyedStream<LoginBean, Long> keyed = watermarks.keyBy(value -> value.getId());

        //4、做出模式pattern
        Pattern<LoginBean, LoginBean> pattern = Pattern.<LoginBean>begin("start").where(new IterativeCondition<LoginBean>() {
            @Override
            public boolean filter(LoginBean value, Context<LoginBean> ctx) throws Exception {
                return value.getState().equals("fail");
            }
        }).times(2).consecutive()
              /*  .next("next").where(new IterativeCondition<LoginBean>() {
                    @Override
                    public boolean filter(LoginBean value, Context<LoginBean> ctx) throws Exception {
                        return value.getState().equals("fail");
                    }
                })*/
                .within(Time.seconds(5));

        //5、在数据流上进行模式匹配
        PatternStream<LoginBean> patternStream = CEP.pattern(keyed, pattern);
        //6、提取匹配成功的数据
        SingleOutputStreamOperator<Long> result = patternStream.process(new PatternProcessFunction<LoginBean, Long>() {
            @Override
            public void processMatch(Map<String, List<LoginBean>> match, Context ctx, Collector<Long> out) throws Exception {
                System.out.println(match);
                out.collect(match.get("start").get(0).getId());
            }
        });

        result.print();

        env.execute();


    }
}
















