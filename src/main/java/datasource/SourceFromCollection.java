package datasource;

import entity.People;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Classname SourceFromCollection
 * @Description 从集合中获取数据源，并且使用pojo类型
 * - 该类是共有且独立的（没有非静态内部类）
 * - 该类有共有的无参构造方法
 * - 类（及父类）中所有的不被static、transient修饰的属性要么有公有的（且不被final修饰），要么是包含共有的getter和setter方法，这些方法遵循java bean命名规范。
 * @Date 2021/11/8 17:37
 * @Created by huni
 */
public class SourceFromCollection {
    public static void main(String[] args) throws Exception {
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        //2.读取数据
        DataStreamSource<String> dataStreamSource = env.fromElements("1","2");
        //3.处理数据
        SingleOutputStreamOperator<People> map1 = dataStreamSource.flatMap(new FlatMapFunction<String, People>() {
            @Override
            public void flatMap(String s, Collector<People> collector) throws Exception {
                System.out.println("ss:"+s);
                String[] s1 = s.split(" ");
                for (String str : s1) {
                    System.out.println("str:"+str);
                    collector.collect(new People(s,1));
                }
            }
        });
        //使用pojo类型的话 就只需要使用字段名称去操作就行

        SingleOutputStreamOperator<People> sumStream = map1.keyBy("name").sum("age");
        //数据输出
        sumStream.print();
        //执行操作
        env.execute();
    }
}
