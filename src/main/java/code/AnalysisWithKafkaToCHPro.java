package code;

import entity.PcWapVo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import util.PropertiesUtil;

import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

import static constant.CommonConstant.*;


/**
 * @Classname AnalysisWithKafkaToRedis
 * @Description
 * 1.flume 监控 /home/bigdata/logs/new_click_all_view.log.2021-12-02文件 输出到kafka
 * 2.从kafka中消费数据
 * 3.过滤掉只拿自助详情页链接
 * 4.flink计算日活（DAU） 步骤：
 *  获取kafka数据
 *  对数据进行业务处理
 *  定义窗口 --一天的
 *  trigger 触发窗口计算
 *  evictor 清空本次窗口的数据
 *  做状态保存结果，进行累加
 *  一个valueState  保存pv
 *  一个MapState 保存人员，进行计算uv
 *
 * @Date 2021/11/24 10:53
 * @author  by huni
 */
public class AnalysisWithKafkaToCHPro {
    public static void main(String[] args) {
     try{
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         //使用日志时间
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
         env.enableCheckpointing(10000);
         env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
         Properties kafkaProps = new Properties();
         kafkaProps.setProperty("bootstrap.servers",  PropertiesUtil.get("metadata.broker.list2"));
         kafkaProps.setProperty("group.id", PropertiesUtil.get("free.pc.groupId"));
         String topic =  PropertiesUtil.get("free.pc.topic");
         FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProps);
         //从当前消费组记录的偏移量开始，接着上次的偏移量消费
         //kafkaSource.setStartFromGroupOffsets();
         kafkaSource.setStartFromLatest();
         //自动提交offset
         kafkaSource.setCommitOffsetsOnCheckpoints(true);
         //添加source 获取数据
         DataStreamSource<String> kafkaSourceData = env.addSource(kafkaSource);
         SingleOutputStreamOperator<PcWapVo> flatMap = kafkaSourceData
                 .filter(x -> {
                     String[] arr = x.split("\u0001");
                     System.out.println(arr[0]);
                     System.out.println("进入时间"+System.currentTimeMillis());
                     String loc = arr[10];
                     String pattern = analysis_pattern;
                     boolean isMatch = Pattern.matches(pattern, URLDecoder.decode(loc, "UTF-8"));
                     if(isMatch){
                         return true;
                     }else {
                         return false;
                     }
                 })
                 .flatMap(new FlatMapFunction<String, PcWapVo>() {
                     @Override
                     public void flatMap(String s, Collector<PcWapVo> collector) {
                         try{
                             String[] arr = s.split("\u0001");
                             String date = arr[0].split(" ")[0];
                             String worker_id = arr[3];
                             String loc = arr[10];
                             String reportId = loc.split("id=")[1].split("&")[0].replaceAll("#", "");
                             Date parseDate = sdf.parse(arr[0]);
                             long ts = parseDate.getTime();
                             if(loc.contains(report_mapping)){
                                 collector.collect(new PcWapVo(loc, "埋点分析",reportId, date,worker_id,ts));
                             }else if(loc.contains(funnel_mapping)){
                                 collector.collect(new PcWapVo(loc, "漏斗分析",reportId, date,worker_id,ts));
                             }else if(loc.contains(newreport_mapping)){
                                 collector.collect(new PcWapVo(loc, "自助分析",reportId,date,worker_id,ts));
                             }else if(loc.contains(retained_mapping)){
                                 collector.collect(new PcWapVo(loc, "留存分析",reportId,date,worker_id,ts));
                             }
                         }catch (Exception e){
                             e.printStackTrace();
                         }
                     }
                 });


         //sink,采用JdbcSink工具类 (flink 1.11 版本提供)
         /**
          * sink方法需要依次传入四个参数：1.预编译的SQL字符串
          * 2.实现JdbcStatementBuilder的对象，用于对预编译的SQL进行传值
          *  3.JDBC执行器，可以设置批量写入等参数，负责具体执行写入操作
          * 4.JDBC连接设置 包含连接驱动、URL、用户名、密码
          * 说明：不同的数据库在jdbc连接设置部分传入不同的驱动、url等参数即可
          */
         String sql =  PropertiesUtil.get("free.pc.sql");
         SinkFunction<PcWapVo> sink = JdbcSink.sink(sql, new MysqlBuilder(),
                 JdbcExecutionOptions.builder().withBatchSize(50).build(),
                 new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                         .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                         .withUrl("jdbc:clickhouse://172.16.5.32:8123/tmp")
                         .withUsername("default")
                         .build());

         /** 这里有个问题就是如果是插入到ch的话，但ch是不支持update和delete的（不友好），然后吧 引擎 设置为 ENGINE = ReplacingMergeTree
          但是他又是不定时更新的。所以一直不能实时起来，后面想的是  吧数据实时落地到ch中，不做处理，在建一个视图来进行count 和count（distinct id）来统计pv,uv
          */
         flatMap.addSink(sink);
         env.execute();

     }catch (Exception e){
         e.printStackTrace();
     }
    }

    //自定义StatementBuilder 实现accept方法实现对预编译的SQL进行传值
    public static class MysqlBuilder implements JdbcStatementBuilder<PcWapVo> {
        @Override
        public void accept(PreparedStatement pst, PcWapVo vo)  {
            try {
                pst.setString(1,vo.getWid());
                pst.setString(2,vo.getReportId());
                pst.setString(3,vo.getLoc());
                pst.setString(4,vo.getType());
                pst.setString(5, Long.toString(vo.getTs()));
                pst.setString(6, vo.getL_date());
                System.out.println("插入时间"+System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
    