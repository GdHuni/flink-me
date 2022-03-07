package real_dw.my_story;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import real_dw.dw.BaseLogApp;
import real_dw.util.CodisUtil;
import real_dw.util.SourceFromKafkaUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huni
 * @Classname KyPrivateHouseInfo
 * @Description
 * 私域流量： 用户在点了我们房源的时候，就会吧这条消息发送给整个区域的经济人，但是当是某个经纪人推送给用户的话就只通知当前经纪人
 * 数据流：Kafka(dwd 客源数据)-->ads (es )
 * {"@timestamp":"2022-03-04T14:47:58.461Z","beat":{"hostname":"bigdata-filebeat-z9t9q","name":"bigdata-filebeat-z9t9q","version":"5.5.1"},"input_type":"log","message":"{\"actionType\":1,\"areaCode\":\"000161\",\"cityName\":\"000120\",\"comName\":\"龙光城北三期\",\"dynamicType\":\"401\",\"fhId\":\"89166801\",\"phone\":\"15626831404\",\"placeCode\":\"000194\",\"price\":\"1500.00\",\"rentSellType\":\"rent\",\"rooms\":\"一房\",\"rtime\":\"2022-03-04 22:47:57\",\"terminal\":4,\"viewTime\":\"2022-03-04 22:47:57\"}","offset":6111120,"source":"/home/logs/bigdatalogs/steward-app-boot/spiderInfo_log_c_steward-app-boot-v1208-135408-6cd67447b8-ccgzk.log","type":"ky-online-view-log"}
 * @Date 2022/3/7 11:52
 */




public class KyPrivateHouseInfo {

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

        //TODO 3.将每行数据转换为JSON对象
        OutputTag<String> dir = new OutputTag<String>("dir"){};
        SingleOutputStreamOperator<JSONObject> jsonData = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //正确数据
                    JSONObject jsonValue = JSONObject.parseObject(value);
                    out.collect( JSONObject.parseObject(jsonValue.getString("message")));
                } catch (Exception e) {
                    //脏数据
                    ctx.output(dir, value);
                }

            }
        });

        //打印脏数据
        jsonData.print("jsonData");

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
                long validTime = CodisUtil.getValidTime();
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.seconds(validTime))
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
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.16.16.250", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {

                        /*用map的方式提交的
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);*/
                        //我采用的是json提交的
                        return Requests.indexRequest()
                                .index("aiky_dynamic")
                                .type("ClientDynamicEs")
                                .source(element, XContentType.JSON);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        // element = "{\"consumerId\":\"1\",\"contentJson\":\"{\\\"msgContent\\\":\\\"这个房子价格多少？\\\",\\\"msgIdClient\\\":\\\"17b3f84e-2473-4178-9b6e-f3fa139a28a1\\\",\\\"msgIdServer\\\":\\\"44891591304\\\"}\",\"dynamicId\":\"6c72c6cbfe1a19dcee9f8a3d92f95e09\",\"dynamicType\":\"201\",\"insertTime\":1616471799632,\"openStatus\":0,\"phone\":\"110001\",\"viewTime\":\"2019-10-15 15:18:00\",\"workerId\":\"06050717\"}";
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        //从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
        esSinkBuilder.setBulkFlushMaxActions(2);
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        // provide a RestClientFactory for custom configuration on the internally created REST client
        /*        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setDefaultHeaders(...);
                    restClientBuilder.setMaxRetryTimeoutMillis(...);
                    restClientBuilder.setPathPrefix(...);
                    restClientBuilder.setHttpClientConfigCallback(...);
                }
        );*/

        // finally, build and add the sink to the job's pipeline
        //从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
        sideOutput.addSink(esSinkBuilder.build()).setParallelism(1);




        //TODO 8.启动任务
        env.execute("keyuan");

    }
    public static class RestClientFactoryImpl implements RestClientFactory {
        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type","application/json")};
            restClientBuilder.setDefaultHeaders(headers); //以数组的形式可以添加多个header
            restClientBuilder.setMaxRetryTimeoutMillis(90000);
        }
    }
}

