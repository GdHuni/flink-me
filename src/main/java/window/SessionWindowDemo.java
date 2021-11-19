package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @Classname SessionWindowDemo
 * @Description 基于会话的窗口
 * @Date 2021/11/10 18:13
 * @Created by huni
 */
public class SessionWindowDemo {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取流式数据
        DataStreamSource<String> data = env.socketTextStream("172.16.5.29", 7777);

        //3.写自己的业务逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();
                int random = new Random().nextInt(10);
                System.out.println("value: " + value + " random: " + random + "timestamp: " + timeMillis + "|" + format.format(timeMillis));
                return new Tuple2<String, Integer>(value, random);
            }
        });

        //根据key分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream  = mapStream.keyBy(key -> key.f0);
        keyedStream.print("keyedStream");
        //定义session的时间窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> sessionWind = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。
        sessionWind.apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            @Override
            /**
             *  data     输入的原始数据
             *  window 窗口、环境相关的数据
             *  iterable 计算后的拥有相同key的数据集合（keyBy后的keyedStream数据）
             *  collector 发送器
             */
            public void apply(String data, TimeWindow window, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                int sum = 0;

                for(Tuple2<String,Integer> tuple2 : iterable){
                    sum +=tuple2.f1;
                }
                long start = window.getStart();
                long end = window.getEnd();
                collector.collect("key:" + data + " value: " + sum + "| window_start :"
                        + format.format(start) + "  window_end :" + format.format(end)
                );
            }
        }).print();

        env.execute();
    }
}
