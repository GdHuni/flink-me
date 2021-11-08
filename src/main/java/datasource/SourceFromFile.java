package datasource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Classname SourceFromFile
 * @Description 从文件获取数据源
 * @Date 2021/11/8 11:57
 * @Created by huni
 */
public class SourceFromFile {
    public static void main(String[] args) throws Exception {
        //String inputHdfsPath = "hdfs://bigdata021200:8020/data/logs/bigdata/alarm/2021-08-09";
        String in = "D:\\源D盘\\最近项目\\a.txt";
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        //2.读取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile(in);
        /**
         * flatMap:Takes one element and produces zero, one, or more elements. A flatmap function that splits sentences to words:
         * map: Takes one element and produces one element. A map function that doubles the values of the input stream:
         */
        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String,Integer>> map1 = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String str : s1) {
                    collector.collect(new Tuple2<String,Integer>(s,1));
                }
            }
        });
/*          上面的flatMap可以替代掉map操作
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });*/
        map1.print("map");
        //就是分流操作，将数据按照一定的规则分到对应的分区
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = map1.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = tuple2StringKeyedStream.sum(1);

        //4.输出数据
        sumStream.print();
        //5.执行代码
        env.execute();

    }
}
