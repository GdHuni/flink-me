package datasource.custom_datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Classname SourceFromKafka
 * @Description  有并行度的自定义数据源，数据会在所有的分区都输出一次
 * @Date 2021/11/8 17:53
 * @Created by huni
 */

public class SelfSourceExtendsRichRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new SelfSourceParallelExtendsRich());
        data.print();
        env.execute();
    }
}
