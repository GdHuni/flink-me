package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Classname CustomKafkaSourceToRedisSink
 * @Description 数据输出到RedisDemo,可以自定义去实现，也可以用flink包装好的RedisSink
 * @Date 2021/11/9 14:51
 * @Created by huni
 */
public class SinkToRedis{
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("D:/data.txt", "testfile");
        //2.读取数据源数据
        //设置kafka配置
        String topic = "huni_topic";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","mygp");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //从当前消费组记录的偏移量开始，接着上次的偏移量消费
        consumer.setStartFromGroupOffsets();
        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);

        //添加数据源
        DataStreamSource<String> data = env.addSource(consumer);
        int parallelism = data.getParallelism();
        System.out.println("...parallelism" + parallelism);
        //处理逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = data.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            //改成了 richFunction  自定义读取文件功能
            @Override
            public void open(Configuration parameters) throws Exception {

                File file = getRuntimeContext().getDistributedCache().getFile("testfile");
                List<String> strings = FileUtils.readLines(file.getAbsoluteFile());
                System.out.println(strings);
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String,Integer>(value,1);


            }
        });
        KeyedStream<Tuple2<String,Integer>, String> keyed = maped.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumValue = keyed.sum(1);


        //4.输出到Redis
        //定义Redis的一些参数
        FlinkJedisPoolConfig.Builder builder = new FlinkJedisPoolConfig.Builder();
        builder.setHost("172.16.21.200");
        builder.setPort(6900);
//        builder.setPassword("lucas");
        FlinkJedisPoolConfig config = builder.build();

        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<>(config, new RedisMapper<Tuple2<String, Integer>>() {
            //定义Redis存储的数据类型及数据
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> data) {
                return data.f1 + "";
            }
        });

        //输出到Redis
        sumValue.addSink(redisSink);
        env.execute();


    }
}
