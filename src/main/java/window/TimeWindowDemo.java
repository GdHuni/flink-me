package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @Classname TimeWindowDemo
 * @Description 时间窗口demo
 * @Date 2021/11/10 11:41
 * @Created by huni
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置使用事件时间
        DataStreamSource<String> data = env.socketTextStream("172.16.5.29", 7777);

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

        KeyedStream<Tuple2<String, Integer>, String> keyedStream  = mapStream.keyBy(key -> key.f0);
        /**
         * window函数里面放的是指派器，可以分为
         *  --时间窗口（可事件时间或者处理时间）
         *      --滚动
         *      --滑动
         *      --回话
         *  --计数窗口
         *      --滚动
         *      --滑动
         */

/*        //时间窗口 --基于事件时间和处理时间 可以直接用timeWindow
        //滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)));
        keyedStream.window(TumblingEventTimeWindows.of(Time.milliseconds(1000)));
        //滑动窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.milliseconds(1000),Time.milliseconds(200)));
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.milliseconds(1000),Time.milliseconds(200)));

        //session窗口
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(10)));

        //计数窗口--可以直接用countWindow
        //滚动
        keyedStream.window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(2)));
        //滑动
        keyedStream.window(GlobalWindows.create()).evictor(CountEvictor.of(2)).trigger(CountTrigger.of(1));*/


        // 滚动窗口 基于时间驱动，每隔10s划分一个窗口,是按照实际时间来的，而不是数据发送的时间，类似10:09:59 发送过来，那么这个窗口到10:10:00就会关闭
      //  WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(20),Time.seconds(1)); //flink-1.12.0已弃用
       //统计周期为一天的数据，因为这个一天的默认时间窗口从8点开始的，时区问题，所以需要减去8小时（这个还是滚动窗口）
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow =  keyedStream.window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                 .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)));
        // 滑动窗口 基于时间驱动，每隔10s划分一个窗口,每5s计算一次，总共计算10s的数据 是按照实际时间来的，而不是数据发送的时间，类似10:09:59 发送过来，那么这个窗口到10:10:00就会关闭
        //WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.milliseconds(10000).Time.milliseconds(5000)); //flink-1.12.0已弃用

        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。进去的是基于key并且窗口的（就是相同的key,在规定的窗口时间进去的数据）
        timeWindow.apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            @Override
            /**
             *  data     输入的原始数据
             *  window 窗口、环境相关的数据
             *  iterable 计算后的拥有相同key的数据集合（keyBy后的keyedStream数据）
             *  collector 发送器
             */
            public void apply(String data, TimeWindow window, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                int sum = 0;

                for(Tuple2<String,Integer> tuple2 : iterable){
                    sum +=tuple2.f1;
                }
                //窗口的开始和结束时间
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
