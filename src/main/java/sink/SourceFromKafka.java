package sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Classname SourceFromKafka
 * @Description  从kafka获取数据
 * @Date 2021/11/8 17:53
 * @Created by huni
 */
public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据源数据
        //设置kafka配置
        String topic = "huni_topic";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id", "zh_24");

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
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMap = data.flatMap(new MyFlatMap()).setParallelism(1);

        //按照key分组策略，对流式数据调用状态化处理

        //输出

        env.execute();

    }
}
