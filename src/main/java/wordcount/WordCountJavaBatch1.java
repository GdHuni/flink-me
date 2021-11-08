package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 1、读取数据源
 *
 * 2、处理数据源
 *
 * a、将读到的数据源文件中的每一行根据空格切分
 *
 * b、将切分好的每个单词拼接1
 *
 * c、根据单词聚合（将相同的单词放在一起）
 *
 * d、累加相同的单词（单词后面的1进行累加）
 *
 * 3、保存处理结果
 */
public class WordCountJavaBatch1 {
    public static void main(String[] args) throws Exception {
        //定义输入输出文件
        String in = "D:\\A_E\\hello.txt";
        String out = "D:\\A_E\\outputJava.txt";
        //准备环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件
        DataSource<String> ds = environment.readTextFile(in);
        //写业务
        FlatMapOperator<String, Tuple2<String, Integer>> tuple = ds.flatMap(new MyCla());
        AggregateOperator<Tuple2<String, Integer>> sum = tuple.groupBy(0).sum(1).setParallelism(2);
        sum.writeAsCsv(out);

        environment.execute();
    }
   static  class MyCla implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] lines = s.split(" ");
            for (String line : lines) {
                collector.collect(new Tuple2<>(line,1));
            }
        }
    }
}


















