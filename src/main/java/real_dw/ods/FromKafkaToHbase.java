package real_dw.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import real_dw.entity.HbaseTradeOrdersVo;
import real_dw.entity.TradeOrdersVo;
import real_dw.util.ReadFromKafkaUtil;
import real_dw.util.SinkToHbase;

public class FromKafkaToHbase {

    public static void main(String[] args) throws Exception {
        //1.设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取kafka数据源
        FlinkKafkaConsumer consumer = ReadFromKafkaUtil.getFlinkKafkaConsumer("tp_2");
        DataStreamSource<String> source = env.addSource(consumer);
        //3.逻辑编写
        SingleOutputStreamOperator<HbaseTradeOrdersVo> map = source.map(x -> {
            JSONObject jsonObject = JSONObject.parseObject(x);
            HbaseTradeOrdersVo vo = new HbaseTradeOrdersVo();
            vo.setTableName(jsonObject.getString("table"));
            vo.setDateBaseName(jsonObject.getString("database"));
            vo.setType(jsonObject.getString("type"));
            vo.setDataInfo(jsonObject.getString("data"));
            return vo;
        });
        map.print();
        //4.数据存储hbase
        map.addSink(new SinkToHbase());


        //4.运行
        env.execute();
    }
}
