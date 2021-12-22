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
 * todo 牢记水印特点：
 * 1.watermark 是一条特殊的数据记录（只用来触发窗口结束，不参与数据计算）
 * 2.watermark 必须单调递增，以确保任务的事件时间时钟在向前推进，而不是在后退（就算延迟数据到来了，比目前的水印小，他也不会倒退了，而是直接不赋值，return掉）
 *  在org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator.emitWatermark 方法中
 * 3.watermark 与数据的时间戳相关
 *  目前遇到一个问题：在下面的水印代码和窗口代码都写好了的时候，发现有时候水印能触发窗口结束，有时候不能，后面发现代码有问题，在我们初始化水印时间的时候使用了
 *     private long maxTimeStamp = Long.MIN_VALUE; 当没数据来的时候，代码运行到这里给他赋值，然后他在减去允许迟到的时间，这时候该值就变成了
 *     导致在emitWatermark 吧该值赋值给了currentWatermark，所以后面再来数据的的时候，数据的eventtime的水印时间都要比这个小，所以都不会触发窗口了。
 *     窗口触发条件，(1)在[window_start_time,window_end_time)窗口中有数据存在(2)watermark时间 >= window_end_time；
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
        DataStreamSource<String> streamSource = env.socketTextStream("linux121", 7777);
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
                    private long maxTimeStamp = 0l;

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
