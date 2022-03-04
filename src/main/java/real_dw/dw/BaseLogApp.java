package real_dw.dw;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import real_dw.util.SourceFromKafkaUtil;

//数据流：Kafka(dwd 客源数据)-->ads (es )

/**
 * 私域流量： 用户在点了我们房源的时候，就会吧这条消息发送给整个区域的经济人，但是当是某个经纪人推送给用户的话就只通知
 * 当前经纪人
 */

public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.消费 ods_base_log 主题数据创建流
        String sourceTopic = "ky-online-view-log";
        String groupId = "base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource( SourceFromKafkaUtil.getFlinkKafkaConsumer(sourceTopic));
        kafkaDS.print();
        //TODO 3.将每行数据转换为JSON对象
        OutputTag<String> dir = new OutputTag<String>("dir"){};
        SingleOutputStreamOperator<JSONObject> jsonData = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //正确数据
                    JSONObject jsonValue = JSONObject.parseObject(value);
                    out.collect(jsonValue);
                } catch (Exception e) {
                    //脏数据
                    ctx.output(dir, value);
                }

            }
        });

        //打印脏数据
        jsonData.getSideOutput(dir).print();


        //TODO 4.状态编程 只有当天（状态24小时）首次访问（过滤非首次访问的）的用户才发消息给经纪人
        //尝试从 checkpoint/savepoint 进行恢复时，TTL 的状态（是否开启）必须和之前保持一致，否则会遇到 “StateMigrationException”。
        SingleOutputStreamOperator<JSONObject> firstData = jsonData.keyBy(data -> data.getString("phone")).filter(new RichFilterFunction<JSONObject>() {
            //定义状态 目前存用户手机号码 设置过期时间为一天
            private ValueState<String> valueState;
            //定义状态描述
            //通过运行环境获取状态

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("phone", String.class);
                //设置状态的超时时间以及更新时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String phone = value.getString("phone");
                if (valueState.value() == null) {
                    //第一次进来
                    valueState.update(phone);
                    return true;
                }
                return false;
            }
        });

        //TODO 5.分流  侧输出流 不带workerid的公共流量 ：主流  带workerid的私域流量
        OutputTag<String> publicData = new OutputTag<String>("publicData"){};
        SingleOutputStreamOperator<String> priData = firstData.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                String workerId = value.getString("workerId");
                //私域流量
                if(StringUtils.isNotBlank(workerId)){
                    out.collect(value.toJSONString());
                }else{
                    //公域流量
                    ctx.output(publicData,value.toJSONString());
                }
            }
        });
        //TODO 6.提取侧输出流
        DataStream sideOutput = priData.getSideOutput(publicData);

        //TODO 7.将三个流进行打印并输出到对应的Kafka主题中
        priData.print();
        sideOutput.print();


        //TODO 8.启动任务
        env.execute("keyuan");

    }

}
