package real_dw.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Classname TradeOrdersFromKafkaToHbase
 * @Description  从kafka获取数据工具类
 * @Date 2022/1/29 16:21
 * @author huni
 */
public class SourceFromKafkaUtil {
    public static FlinkKafkaConsumer getFlinkKafkaConsumer(String topic) {
        FlinkKafkaConsumer consumer=null;
        try{
            //2.读取kafka数据源 配置kafka信息
            Properties props = new Properties();
            String servers = PropertiesUtil.readValue("bootstrap.servers");
            props.setProperty("bootstrap.servers",servers);
            props.setProperty("group.id","mygp");
            consumer = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),props);
            //从当前消费组记录的偏移量开始，接着上次的偏移量消费
            consumer.setStartFromGroupOffsets();
            //自动提交offset
            consumer.setCommitOffsetsOnCheckpoints(true);
        }catch (Exception e){
            e.printStackTrace();
        }

        return consumer;
    }
}
