package code;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;
import scala.tools.nsc.doc.model.Val;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Classname FlinkWindowCode
 * @Description  window相关题目
 * 标题：滑动窗口最大和 | 时间限制：1秒|内存限制：262144K | 语言限制：不限
 * 【滑动窗口最大和】有一个N个整数的数组，和一个长度为M的窗口，窗口从数组内的第一个数开始滑动直到窗口不能滑动为止，
 * 每次窗口滑动产生一个窗口和（窗口内所有数的和），求窗口滑动产生的所有窗口和的最大值。
 * 输入描述：
 * 第一行输入一个正整数N，表示整数个数。(0<N<100000)
 * 第二行输入N个整数，整数的取值范围为[-100,100]
 * 第三行输入一个正整数M， M代表窗口大小，M<=100000，且M<=N。
 * 输出描述
 * 窗口滑动产生的所有窗口和的最大值。
 * 示例1，
 * 输入
 * 6
 * 10 20 30 15 23 12
 * 3
 * 输出
 * 68
 *
 *
 *
 * @Date 2021/11/25 17:15
 * @Created by huni
 */
public class FlinkWindowCode {

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        String s = sc.nextLine();
        char[] chars = s.toCharArray();
        Set set = new HashSet();
        for (char aChar : chars) {
            set.add(aChar);
        }
        System.out.println(set.size());
        //flink窗口方式计算
       // flinkCount();
        //javaCount();

    }

    public static void javaCount() {
        Integer[] s = {10,20,30,15,23,12};
        Map map = new HashMap<>();
        List<Integer> integers = Arrays.asList(s);
        int num = 0;
        int sum = 0;
        Map<Integer,Integer> m = new HashMap();
        for (Integer i : integers) {
            num++;
            if(num<=3){
                m.put(num,i);
                //存放总数
                sum= sum+i;
            }else {
              if(num%3==2){
                  m.put(1,i);
              }else if(num%3==1){
                  m.put(2,i);
              }else {
                  m.put(3,i);
              }
                Integer currSum = m.get(1).intValue()+ m.get(2).intValue()+ m.get(3).intValue();
                sum=  sum>=currSum?sum:currSum;
            }
        }
        System.out.println(sum);
    }

    public static void flinkCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Map input = input();
        DataStreamSource<Integer> source = env.fromElements(10,20,30,15,23,12);
        SingleOutputStreamOperator<Tuple2<Integer,Integer>> mapStream = source.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer value) throws Exception {
                return new Tuple2<>(1, value);
            }
        });
        mapStream.print();
        KeyedStream<Tuple2<Integer,Integer>, Integer> keyedStream = mapStream.keyBy(x -> x.f0);
        WindowedStream<Tuple2<Integer, Integer>, Integer, GlobalWindow> countWindow = keyedStream.countWindow(3, 1);
        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。输入数据  输出数据 key window

        countWindow.apply(new WindowFunction<Tuple2<Integer, Integer>, Integer, Integer, GlobalWindow>() {
            int count = 1;
            int maxSum = 0;
            @Override
            /**
             *  data     输入的原始数据
             *  window 窗口、环境相关的数据
             *  iterable 计算后的拥有相同key的数据集合（keyBy后的keyedStream数据）
             *  collector 发送器
             */
            public void apply(Integer data, GlobalWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<Integer> out) throws Exception {
                int sum = 0;
                count++;
                for (Tuple2<Integer, Integer> tuple2 : input){
                    sum += tuple2.f1;
                }
                maxSum = maxSum>sum?maxSum:sum;
                if(count >=7){
                    out.collect(maxSum);
                }
            }
        }).print();
        env.execute();

    }
    public static Map input(){
        List list = new LinkedList<>();
        Integer[] s = {10,20,30,15,23,12};
        Map map = new HashMap<>();
        map.put(1,6);
        map.put(2, Arrays.asList(s));
        map.put(3,3);
        return map;
    }
}
