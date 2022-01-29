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
 * @Description 使用flink计算 日活，这个案列是计算报表平台 的针对各个报表的日活输出到clickhouse
 *  * 1.flume 监控 /home/bigdata/logs/new_click_all_view.log.2021-12-02文件 输出到kafka
 *  * 2.从kafka中消费数据
 *  * 3.过滤掉只拿自助详情页链接
 *  * 4.flink计算日活（DAU） 步骤：
 *  *  获取kafka数据
 *  *  对数据进行业务处理
 *  *  定义窗口 --一天的
 *  *  trigger 触发窗口计算
 *  *  evictor 清空本次窗口的数据
 *  *  做状态保存结果，进行累加
 *  *  一个valueState  保存pv
 *  *  一个MapState 保存人员，进行计算uv
 * @Date 2021/12/3 18:47
 * @Created by huni
 */
public class DAU {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //使用日志时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 如果在只做快照过程中出现错误，是否让整体任务失败：true是  false不是
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        env.setStateBackend(new FsStateBackend("hdfs://bigdata021200:8020/user/check"));
        Properties kafkaProps = new Properties();


        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093");
        props.setProperty("group.id","zh_test_1203");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("pc_free_huni", new SimpleStringSchema(), props);

        //从当前消费组记录的偏移量开始，接着上次的偏移量消费
        //kafkaSource.setStartFromGroupOffsets();
        consumer.setStartFromLatest();
        //自动提交offset
        consumer.setCommitOffsetsOnCheckpoints(true);
        //添加source 获取数据
        DataStreamSource<String> kafkaSourceData = env.addSource(consumer);
        SingleOutputStreamOperator<Tuple2<PcWapVo, Integer>> flatMap = kafkaSourceData
                //.filter(x -> URLDecoder.decode(x, "UTF-8").contains("/analysis/")&&URLDecoder.decode(x, "UTF-8").contains("id="))
                .flatMap(new FlatMapFunction<String, Tuple2<PcWapVo, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<PcWapVo, Integer>> collector) throws Exception {
                        //使用java.net.URLDecoder.decode()进行编码
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
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "埋点分析",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/funnel/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "漏斗分析",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/newreport/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "自助分析",reportId,date,wid,ts), 1));
                            }else if(loc.contains("analysis/retained/detail?id")){
                                collector.collect(new Tuple2<>(new PcWapVo(loc, "留存分析",reportId,date,wid,ts), 1));
                            }
                        }
                    }
                });
        KeyedStream<Tuple2<PcWapVo, Integer>, String> KeyedStream = flatMap.keyBy(x -> x.f0.getType()+"_"+x.f0.getL_date()+"_"+x.f0.getReportId());
        SingleOutputStreamOperator<PcWapVo> process = KeyedStream.window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(100)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new ProcessWindowFunction<Tuple2<PcWapVo, Integer>, PcWapVo, String, TimeWindow>() {
                    /* 如果长时间的窗口，比如：一天的窗口
                            所有大窗口会添加trigger，以一定的频率输出中间结果。
                            加evictor 是因为，每次trigger，触发计算是，窗口中的所有数据都会参与，所以数据会触发很多次，比较浪费，加evictor 驱逐已经计算过的数据，就不会重复计算了
                            驱逐了已经计算过的数据，导致窗口数据不完全，所以需要state 存储我们需要的中间结果*/
                    ValueState<Tuple2<String, Long>> pvCount;
                    MapState<String,String> uvCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //在open方法中做出State
                        ValueStateDescriptor<Tuple2<String, Long>> pvCountDes = new ValueStateDescriptor<>(
                                "pvCount",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                })
                        );
                        MapStateDescriptor wordStateDes = new MapStateDescriptor<>(
                                "uvCount",
                                TypeInformation.of(new TypeHint<String>() {}),TypeInformation.of(new TypeHint<String>() {})
                        );


                        //RuntimeContext是Function运行时的上下文，包含了Function在运行时需要的 所有信息，如并行度相关信息、Task名称、执行配置信息ExecutionConfig、State等
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
        /** 这里有个问题就是如果是插入到ch的话，但ch是不支持update和delete的（不友好），然后吧 引擎 设置为 ENGINE = ReplacingMergeTree
                 但是他又是不定时更新的。所以一直不能实时起来，后面想的是  吧数据实时落地到ch中，不做处理，在建一个视图来进行count 和count（distinct id）来统计pv,uv

                 */

        //sink,采用JdbcSink工具类 (flink 1.11 版本提供)
        /**
         * sink方法需要依次传入四个参数：1.预编译的SQL字符串
         * 2.实现JdbcStatementBuilder的对象，用于对预编译的SQL进行传值
         *  3.JDBC执行器，可以设置批量写入等参数，负责具体执行写入操作
         * 4.JDBC连接设置 包含连接驱动、URL、用户名、密码
         * 说明：不同的数据库在jdbc连接设置部分传入不同的驱动、url等参数即可
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

    //自定义StatementBuilder 实现accept方法实现对预编译的SQL进行传值
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
