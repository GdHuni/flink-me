package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Classname wordcount.WordCountJavaStream
 * @Description java版本的flink流处理
 * @Date 2021/10/28 19:58
 * @Created by huni
 */
public class WordCountJavaStream {
    public static void main(String[] args) {
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.接受端口数据
        DataStreamSource<String> ds = env.socketTextStream("linux121", 7777);
        //3.处理数据，计算单词
        SingleOutputStreamOperator<Tuple2<String,Integer>> sum = ds.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String line : s1) {
                    collector.collect(new Tuple2<String, Integer>(line, 1));
                }
            }}) .keyBy(0).sum(1);
        //4.输出
        sum.print();
        //5.运行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
