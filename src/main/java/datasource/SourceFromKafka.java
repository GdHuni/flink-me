package datasource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Classname SourceFromKafka
 * @Description  从kafka获取数据
 * @Date 2021/11/8 17:53
 * @Created by huni
 */
public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据源数据
        //设置kafka配置
        String topic = "huni_topic";
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","mygp");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //设置从最新的offset开始消费
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);
        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);
        //从指定offset位置开始消费
        /*HashMap<KafkaTopicPartition, Long> specifiStartOffsets = new HashMap<>();
        specifiStartOffsets.put(new KafkaTopicPartition("animal",0),558L);
//        specifiStartOffsets.put(new KafkaTopicPartition("animal",1),0L);
//        specifiStartOffsets.put(new KafkaTopicPartition("animal",2),43L);
        consumer.setStartFromSpecificOffsets(specifiStartOffsets);*/

        //添加数据源
        DataStreamSource<String> data = env.addSource(consumer);
        int parallelism = data.getParallelism();
        System.out.println("...parallelism" + parallelism);
        data.print();
      /*  int parallelism = data.getParallelism();
        System.out.println("...parallelism" + parallelism);
        //处理逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String,Integer>(value,1);


            }
        });
        KeyedStream<Tuple2<String,Integer>, String> keyed = maped.keyBy(value -> value.f0);

        //按照key分组策略，对流式数据调用状态化处理
*//*        SingleOutputStreamOperator<Tuple2<Long, Long>> flatMaped = keyed.flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            ValueState<Tuple2<Long, Long>> sumState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //在open方法中做出State
                ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }),
                        Tuple2.of(0L, 0L)
                );

                sumState = getRuntimeContext().getState(descriptor);
//                super.open(parameters);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
                //在flatMap方法中，更新State
                Tuple2<Long, Long> currentSum = sumState.value();

                currentSum.f0 += 1;
                currentSum.f1 += value.f1;

                sumState.update(currentSum);
                out.collect(currentSum);


                *//**//*if (currentSum.f0 == 2) {
                    long avarage = currentSum.f1 / currentSum.f0;
                    out.collect(new Tuple2<>(value.f0, avarage));
                    sumState.clear();
                }*//**//*

            }
        });*//*
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumValue = keyed.sum(1);
        //输出
        sumValue.print();*/
        env.execute();

    }
}
