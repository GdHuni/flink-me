import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.Random;

public class MonitorLogin {
    public static void main(String[] args) throws Exception {
        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream("linux121", 7777);
        KeyedStream<Tuple2<String, String>, String> keyedStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();
                System.out.println("value: " + value  + "timestamp: " + timeMillis + "|" + format.format(timeMillis));
                return new Tuple2<String, String>("1", value);
            }
        }).keyBy(x -> x.f0);

        WindowedStream<Tuple2<String, String>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(10), Time.seconds(1));
        SingleOutputStreamOperator<String> apply = timeWindow.apply(new WindowFunction<Tuple2<String, String>, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<String> out) throws Exception {
                int sum = 0;
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (Tuple2<String, String> in : input) {
                    if(in.f1.equals("f")){
                        sum++;
                    }
                }
                //窗口的开始和结束时间
                long start = window.getStart();
                long end = window.getEnd();
                if(sum>=3){
                    out.collect("key:" + s  + " value: " + sum + "| window_start :"
                            + format.format(start) + "  window_end :" + format.format(end)
                    );
                }/*else {
                    out.collect("成功");
                }
*/
            }
        });
        apply.print();
        env.execute();
    }
}
