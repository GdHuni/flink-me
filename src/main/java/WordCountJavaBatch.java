import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
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
public class WordCountJavaBatch {
    public static void main(String[] args) throws Exception {
        String inputPath = "D:\\A_E\\hello.txt";
        String outputPath = "D:\\A_E\\output";

        //获取flink的运行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = executionEnvironment.readTextFile(inputPath);
        FlatMapOperator<String, Tuple2<String, Integer>> wordOndOnes = text.flatMap(new SplitClz());

        //(hello 1) (you 1) (hi 1) (him 1)
        UnsortedGrouping<Tuple2<String, Integer>> groupedWordAndOne = wordOndOnes.groupBy(0);
        //(hello 1) (hello 1)
        AggregateOperator<Tuple2<String, Integer>> out = groupedWordAndOne.sum(1);

        out.writeAsCsv(outputPath, "\n", " ").setParallelism(1);

        executionEnvironment.execute();

    }

    static class SplitClz implements FlatMapFunction<String, Tuple2<String,Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for(String word : s1) {
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}


















