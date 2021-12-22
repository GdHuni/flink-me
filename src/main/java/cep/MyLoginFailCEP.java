package cep;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 要指定水印
 */
public class MyLoginFailCEP {
    public static void main(String[] args) throws Exception {
        //1.获取数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //一定记得要加这个不然在不同的分区中，数据采集不到那么多，就不会预警
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置kafka配置
        String topic = "topic_1";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","linux121:9092");
        props.setProperty("group.id","mygp");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //设置从最新的offset开始消费
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);

        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);

        DataStream<LoginEvent> loginEventStream = env.addSource(consumer)
                .map(line -> {
                    String[] fields = line.split(",");
                    LoginEvent loginEvent = new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                    return loginEvent;
                });


        //2.添加水印。允许最大延迟2s
        SingleOutputStreamOperator<LoginEvent> watermarks = loginEventStream.assignTimestampsAndWatermarks(new WatermarkStrategy<LoginEvent>() {
            @Override
            public WatermarkGenerator<LoginEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<LoginEvent>() {
                    long maxTimeStamp = 0L;

                    @Override
                    public void onEvent(LoginEvent event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTimestamp());
                    }
                    //最大延迟时间（cep里面的这个水印会直接出发event-2s的时间窗口，当后面迟到的时间以这个时间差大于最大延迟时间适时候，那么那批数据将不会被处理了，下面举例说明）
                    /*
                    * 1234,10.0.1.3,fail,1611373943000
                      1234,10.0.1.4,success,1611373944500-- 这个就直接触发了 1611373942500之前的时间窗口了，那么后面再出现1611373942500之前的数据也不会统计了
                      1234,10.0.1.3,fail,1611373944000--迟到数据
                      1234,10.0.1.4,fail,1611373944100--迟到数据
                      1234,10.0.1.4,fail,1611373946000
                      1234,10.0.1.4,fail,1611373947000
                      *
                      * 因为第一条数据与第二条数据差没到2s,所以第二条数据触发的窗口1611373942500还没达到1611373943000 所有第一条数据能与后面两条迟到的数据满足规则
                      1234,10.0.1.3,fail,1611373943000
                      1234,10.0.1.4,success,1611373945500-- 这个就直接触发了 1611373943500之前的时间窗口了，那么后面再出现1611373943500之前的数据也不会统计了
                      1234,10.0.1.3,fail,1611373944000--迟到数据
                      1234,10.0.1.4,fail,1611373944100--迟到数据
                      1234,10.0.1.4,fail,1611373946000
                      1234,10.0.1.4,fail,1611373947000
                      *  因为第一条数据与第二条数据差大于2s,所以第二条数据1611373945500水印触发的窗口1611373943500>1611373943000 所有第一条数据能与后面两条迟到的数据不能满足规则
                      * 而有时候我们为什么能看到输出呢？ 因为我们是读取文件或者直接在文件流中，吧所有数据全部输出了（没按照时间戳一下下输出），这样获取的水印就直接是最后一个了，前面的就都能输出
                    * */
                    long maxOutOfOrderness = 2000l;

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp()));

        //3、做出模式pattern
/*        Pattern<LoginEvent, LoginEvent> loginFailPattern1 = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return loginEvent.getLoginState().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                return loginEvent.getLoginState().equals("fail");
            }
        }).within(Time.seconds(10));*/


        /**
         * 10s内 通一个账户连续出现3次失败 就预警
         */
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
        @Override
        public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
            return loginEvent.getLoginState().equals("fail");
        }

        })
            /*出现的次数*/
            .times(3)
            /*与{@link Pattern#oneOrMore（）}或{@link Pattern#times（int）}结合使用。指定任何不匹配的元素都会打断循环。*/
            .consecutive()
            //规定的时间内
            .within(Time.seconds(10));

        // 4. 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(watermarks.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 5. 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning1());

        warningStream.print();
        env.execute();

    }
    // 实现自定义的PatternSelectFunction
    public static class LoginFailMatchDetectWarning1 implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstFailEvent = pattern.get("begin").get(0);
            LoginEvent lastFailEvent = pattern.get("begin").get(pattern.get("begin").size() - 1);
            return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(), lastFailEvent.getTimestamp(), "login fail " + pattern.get("begin").size() + " times");
        }
    }
}
