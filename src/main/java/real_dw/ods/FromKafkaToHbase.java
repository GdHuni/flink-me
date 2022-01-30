package real_dw.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import real_dw.entity.TradeOrdersVo;
import real_dw.util.ReadFromKafkaUtil;

public class FromKafkaToHbase {

    public static void main(String[] args) throws Exception {
        //1.设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取kafka数据源
        FlinkKafkaConsumer consumer = ReadFromKafkaUtil.getFlinkKafkaConsumer("tp_1");

        DataStreamSource<String> source = env.addSource(consumer);
        source.map(x->{
            JSONObject jsonObject = JSONObject.parseObject(x);
            JSONArray data = JSONArray.parseArray(jsonObject.getString("data"));
            TradeOrdersVo tradeOrders = JSON.toJavaObject(JSONObject.parseObject(data.get(0).toString()), TradeOrdersVo.class);
            return tradeOrders;
        });
        //3.逻辑编写


        source.print();

        //4.运行
        env.execute();
    }
}
