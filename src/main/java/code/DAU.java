package code;

import entity.PcWapVo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

/**
 * @Classname DAU
 * @Description ??????flink?????? ?????????????????????????????????????????? ???????????????????????????????????????clickhouse
 *  * 1.flume ?????? /home/bigdata/logs/new_click_all_view.log.2021-12-02?????? ?????????kafka
 *  * 2.???kafka???????????????
 *  * 3.????????????????????????????????????
 *  * 4.flink???????????????DAU??? ?????????
 *  *  ??????kafka??????
 *  *  ???????????????????????????
 *  *  ???????????? --?????????
 *  *  trigger ??????????????????
 *  *  evictor ???????????????????????????
 *  *  ????????????????????????????????????
 *  *  ??????valueState  ??????pv
 *  *  ??????MapState ???????????????????????????uv
 * @Date 2021/12/3 18:47
 * @Created by huni
 */
public class DAU {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //??????????????????
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // ??????checkpoint???????????????
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // ???????????????????????????????????????????????????????????????????????????true???  false??????
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        env.setStateBackend(new FsStateBackend("hdfs://bigdata021200:8020/user/check"));
        Properties kafkaProps = new Properties();


        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","zh_test_1203");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("pc_free_huni", new SimpleStringSchema(), props);

        //???????????????????????????????????????????????????????????????????????????
        //kafkaSource.setStartFromGroupOffsets();
        consumer.setStartFromLatest();
        //????????????offset
        consumer.setCommitOffsetsOnCheckpoints(true);
        //??????source ????????????
        DataStreamSource<String> kafkaSourceData = env.addSource(consumer);
        SingleOutputStreamOperator<Tuple2<PcWapVo, Integer>> flatMap = kafkaSourceData
                //.filter(x -> URLDecoder.decode(x, "UTF-8").contains("/analysis/")&&URLDecoder.decode(x, "UTF-8").contains("id="))
                .flatMap(new FlatMapFunction<String, Tuple2<PcWapVo, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<PcWapVo, Integer>> collector) throws Exception {
                        //??????java.net.URLDecoder.decode()????????????
                        String keyWord = URLDecoder.decode(s, "UTF-8");
                        // System.out.println(keyWord);
                        String[] split = keyWord.split("\u0001");
                        if (split.length >= 3) {
                            String loc = split[2].split("&loc=")[1];
                            String wid = split[2].split("&wid=")[1].split("&mac=")[0];
                            String reportId = loc.split("id=")[1].split("&")[0].replaceAll("#", "");
                            String date = split[0].split(" ")[0];
                            Date date1 = sdf.parse(split[0]);
                            long ts = date1.getTime();
                            if(loc.contains("analysis/report/toDetail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "????????????",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/funnel/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "????????????",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/newreport/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "????????????",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/retained/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "????????????",reportId,date,wid,ts), 1));
                            }
                        }
                    }
                });
        KeyedStream<Tuple2<PcWapVo, Integer>, String> KeyedStream = flatMap.keyBy(x -> x.f0.getType()+"_"+x.f0.getL_date()+"_"+x.f0.getReportId());
        SingleOutputStreamOperator<PcWapVo> process = KeyedStream.window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(100)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new ProcessWindowFunction<Tuple2<PcWapVo, Integer>, PcWapVo, String, TimeWindow>() {
                    /* ???????????????????????????????????????????????????
                            ????????????????????????trigger??????????????????????????????????????????
                            ???evictor ??????????????????trigger???????????????????????????????????????????????????????????????????????????????????????????????????????????????evictor ?????????????????????????????????????????????????????????
                            ??????????????????????????????????????????????????????????????????????????????state ?????????????????????????????????*/
                    ValueState<Tuple2<String, Long>> pvCount;
                    MapState<String,String> uvCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //???open???????????????State
                        ValueStateDescriptor<Tuple2<String, Long>> pvCountDes = new ValueStateDescriptor<>(
                                "pvCount",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                })
                        );
                        MapStateDescriptor wordStateDes = new MapStateDescriptor<>(
                                "uvCount",
                                TypeInformation.of(new TypeHint<String>() {}),TypeInformation.of(new TypeHint<String>() {})
                        );


                        //RuntimeContext???Function?????????????????????????????????Function????????????????????? ??????????????????????????????????????????Task???????????????????????????ExecutionConfig???State???
                        pvCount = getRuntimeContext().getState(pvCountDes);
                        uvCount = getRuntimeContext().getMapState(wordStateDes);
                        super.open(parameters);
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<PcWapVo, Integer>> elements, Collector<PcWapVo> out) throws Exception {
                        long pv = 0;
                        for (Tuple2<PcWapVo, Integer> element : elements) {
                            pv ++;
                            String word = element.f0.getWid();
                            uvCount.put(word, null);
                        }
                        if(null == pvCount.value()){
                            pvCount.update(new Tuple2<>(s,pv));
                        }else{
                            pvCount.update(new Tuple2<>(s,pvCount.value().f1 + pv));
                        }

                        long  count  = 0;
                        Iterator<String> iterator = uvCount.keys().iterator();
                        while (iterator.hasNext()) {
                            iterator.next();
                            count += 1;
                        }
                        // uv
                        PcWapVo vo = elements.iterator().next().f0;
                        vo.setPv(pvCount.value().f1);
                        vo.setUv(count);
                        out.collect(vo);
                    }
                });
        /** ??????????????????????????????????????????ch????????????ch????????????update???delete?????????????????????????????? ?????? ????????? ENGINE = ReplacingMergeTree
                 ????????????????????????????????????????????????????????????????????????????????????  ????????????????????????ch????????????????????????????????????????????????count ???count???distinct id????????????pv,uv

                 */

        //sink,??????JdbcSink????????? (flink 1.11 ????????????)
        /**
         * sink???????????????????????????????????????1.????????????SQL?????????
         * 2.??????JdbcStatementBuilder?????????????????????????????????SQL????????????
         *  3.JDBC??????????????????????????????????????????????????????????????????????????????
         * 4.JDBC???????????? ?????????????????????URL?????????????????????
         * ??????????????????????????????jdbc??????????????????????????????????????????url???????????????
         */
        String sql = "insert into tmp.jdbc_example (loc,pv,date) values (?,?,?)";
        SinkFunction<PcWapVo> sink = JdbcSink.sink(sql, new MysqlBuilder(),
                JdbcExecutionOptions.builder().withBatchSize(50).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl("jdbc:clickhouse://172.16.5.32:8123/tmp")
                        .withUsername("default")
                        //.withPassword("root")
                        .build());

        process.addSink(sink);

        env.execute();

    }

    //?????????StatementBuilder ??????accept???????????????????????????SQL????????????
    public static class MysqlBuilder implements JdbcStatementBuilder<PcWapVo> {

        @Override
        public void accept(PreparedStatement pst, PcWapVo vo)  {
            try {
                pst.setString(1,vo.getLoc());
                pst.setLong(2,vo.getPv());
                pst.setString(3, vo.getL_date());
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
