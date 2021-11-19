package sink.custom_sink;

import entity.People;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @Classname CustomSinkToMysql
 * @Description 数据输出到mysql
 * @Date 2021/11/9 15:03
 * @Created by huni
 */
public class CustomSinkToMysql extends RichSinkFunction {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String url = "jdbc:mysql://172.16.4.32:33098/hu?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
        String user = "root";
        String password = "passwd4_32";

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

        DataStreamSource<String> data = env.addSource(consumer);
        int parallelism = data.getParallelism();
        System.out.println("...parallelism" + parallelism);
        //处理逻辑
        SingleOutputStreamOperator<People> maped = data.map(new MapFunction<String, People>() {
            @Override
            public People map(String value) throws Exception {
                if(!value.contains(",")){
                    return null;
                }
                String[] split = value.split(",");
                return new People(split[0],Integer.valueOf(split[1]));
            }
        });

        maped.addSink(new RichSinkFunction<People>() {
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(url, user, password);
                String sql = "insert into people (name,age) values (?,?)";
                preparedStatement = connection.prepareStatement(sql);
            }

            @Override
            public void invoke(People value, Context context) throws Exception {
                if(null == value){
                    System.out.println("数据异常");
                    return;
                }
                preparedStatement.setString(1,value.getName());
                preparedStatement.setInt(2,value.getAge());
                preparedStatement.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if(connection != null) {
                    connection.close();
                }
                if(preparedStatement != null) {
                    preparedStatement.close();
                }
            }
        });
        env.execute();
    }

}
