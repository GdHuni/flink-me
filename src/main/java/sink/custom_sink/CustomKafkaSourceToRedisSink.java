package sink.custom_sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * @Classname CustomKafkaSourceToRedisSink
 * @Description 从kafka获取数据输出到RedisDemo
 * @Date 2021/11/9 14:51
 * @Created by huni
 */
public class CustomKafkaSourceToRedisSink {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据源数据
        //设置kafka配置
        String topic = "huni_topic";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","mygp");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //设置从最新的offset开始消费
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);
        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);

        //添加数据源
        DataStreamSource<String> data = env.addSource(consumer);
        int parallelism = data.getParallelism();
        System.out.println("...parallelism" + parallelism);
        //处理逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String,Integer>(value,1);


            }
        });
        KeyedStream<Tuple2<String,Integer>, String> keyed = maped.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumValue = keyed.sum(1);


        //输出到Redis
        sumValue.addSink(new CustomRedisSinkFunction()).name("my reids sink");
        env.execute();


    }
}
