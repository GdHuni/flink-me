package watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Classname WaterMarkDemo
 * @Description 水印机制demo代码
 * @Date 2021/11/19 17:28
 * @Created by huni
 */
public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //周期性的生成 watermark：系统会周期性的将 watermark 插入到流中 默认周期是200毫秒
        env.getConfig().setAutoWatermarkInterval(1000L);
        //设置并行度
        env.setParallelism(1);
        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("172.16.5.29", 7777);
        //3.逻辑代码
        SingleOutputStreamOperator<Tuple2<String, Long>> maped = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<String, Long>(split[0], Long.valueOf(split[1]));
            }
        });

        //定义水印
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = maped.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimeStamp = Long.MIN_VALUE;

                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.f1);
                        System.out.println("maxTimeStamp:" + maxTimeStamp + "...format:" + sdf.format(maxTimeStamp));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                      //  System.out.println(".....onPeriodicEmit....");
                        long maxOutOfOrderness = 3000l;
                        output.emitWatermark(new Watermark(maxTimeStamp - maxOutOfOrderness));
                    }
                };
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                return element.f1;
            }
        }));
        //分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = watermarks.keyBy(value -> value.f0);
        //定义窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(4));
        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。进去的是基于key并且窗口的（就是相同的key,在规定的窗口时间进去的数据）
        SingleOutputStreamOperator<Object> result = timeWindow.apply(new WindowFunction<Tuple2<String, Long>, Object, String, TimeWindow>() {
            /**
             *  s     输入的原始数据
             *  window 窗口、环境相关的数据
             *  input 计算后的拥有相同key的数据集合（keyBy后的keyedStream数据）
             *  out 发送器
             */
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Object> out) throws Exception {
                System.out.println("..." + sdf.format(window.getStart()));
                String key = s;
                Iterator<Tuple2<String, Long>> iterator = input.iterator();
                ArrayList<Long> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Long> next = iterator.next();
                    list.add(next.f1);
                }
                Collections.sort(list);
                String result = "key:" + key + "..." + "list.size:" + list.size() + "...list.first:" + sdf.format(list.get(0)) + "...list.last:" + sdf.format(list.get(list.size() - 1)) + "...window.start:" + sdf.format(window.getStart()) + "..window.end:" + sdf.format(window.getEnd());
                out.collect(result);
            }
        });
        result.print();
        env.execute();


    }
}
