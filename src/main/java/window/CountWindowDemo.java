package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @Classname CountWindowDemo
 * @Description 基于数量的窗口
 * @Date 2021/11/10 16:31
 * @Created by huni
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> data = env.socketTextStream("172.16.5.29", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();
                int random = new Random().nextInt(10);
                System.out.println("value: " + value + " random: " + random + " timestamp: " + timeMillis + "|" + format.format(timeMillis));
                return new Tuple2<String, Integer>(value, random);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream  = mapStream.keyBy(key -> key.f0);


        // 滚动窗口基于事件驱动，每隔2个key相同的数据划分一个窗口
        //WindowedStream<Tuple2<String, Integer>, String , GlobalWindow> countWindow = keyedStream.countWindow(2);

        // 滑动窗口，每隔3个key相同的数据划分一个窗口,每次出现两个就滑动一次，所以第一次计算的是1,2两个数据，第二次计算钱四个的，后面的话就是滑动两个在加上前三个组成五个的窗口计算
        //可以简单理解为 slide大小是出来几个数据触发计算的，而size是触发后计算的数量。每来两个就计算，每次计算五个数据
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countWindow = keyedStream.countWindow(5,2);

        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。输入数据  输出数据 key window
        countWindow.apply(new WindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow>() {
            @Override
            /**
             *  data     输入的原始数据
             *  window 窗口、环境相关的数据
             *  iterable 计算后的拥有相同key的数据集合（keyBy后的keyedStream数据）
             *  collector 发送器
             */
            public void apply(String data, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                int sum = 0;

                for (Tuple2<String, Integer> tuple2 : input){
                    sum += tuple2.f1;
                }
                //无用的时间戳，默认值为： Long.MAX_VALUE,因为基于事件计数的情况下，不关心时间。
                long maxTimestamp = window.maxTimestamp();

                out.collect("key:" +data + " value: " + sum + "| maxTimeStamp :"
                        + maxTimestamp + "," + format.format(maxTimestamp)
                );
            }
        }).print();
        env.execute();


    }
}
