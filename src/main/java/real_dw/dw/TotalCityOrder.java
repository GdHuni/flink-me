package real_dw.dw;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import real_dw.entity.DimAreaVo;
import real_dw.entity.DimTradeOrder;
import real_dw.util.SourceFromHbaseUtil;


/**
 * 需求1 :  查询城市、省份、订单总额、订单总数----全量查询
 * 获取两部分数据
 * 1、dim_lagou_area  dim维表数据
 * 2、增量数据   lagou_trade_orders(HBase)
 * 进行计算
 *      1，2 统一到一起参与计算  sql
 *      //把获取到的数据 转成flinktable中的临时表
 *
 */
public class TotalCityOrder {

    public static void main(String[] args) {
        try {
            //1.设置环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //测试设置1
            env.setParallelism(1);
            //设置检查点，时间类型，语义
            env.enableCheckpointing(5000);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            //2.从hbase中读取地域维表数据的数据
            DataStreamSource<Tuple2<String, String>> dimArea = env.addSource(new SourceFromHbaseUtil("dim_area"));
            //增量数据 trade_orders(HBase)订单表获取数据
            DataStreamSource<Tuple2<String, String>> orders = env.addSource(new SourceFromHbaseUtil("trade_orders"));

            //3.将hbase读取的数据转为对象
            //将流数据转为表类型数据
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            SingleOutputStreamOperator<DimAreaVo> areaStream = dimArea.map(x -> {
                int arearId = Integer.parseInt(x.f0);
                String[] datas = x.f1.split("#-#");
                String aname = datas[0].trim();
                int cid = Integer.parseInt(datas[1].trim());
                String city = datas[2].trim();
                int proid = Integer.parseInt(datas[3].trim());
                String province = datas[4].trim();
                return new DimAreaVo(arearId, aname, cid, city, proid, province);
            });
            tableEnv.createTemporaryView("dim_lagou_area",areaStream);

            SingleOutputStreamOperator<DimTradeOrder> orderStream = orders.map(x -> {
                int orderid = Integer.parseInt(x.f0);
                String[] datas = x.f1.split("#-#");
                String orderNo = datas[6].trim();
                int userId = Integer.parseInt(datas[14].trim());
                int status = Integer.parseInt(datas[10].trim());
                Double totalMoney = Double.parseDouble(datas[11]);
                int areaId = Integer.parseInt(datas[0]);
                return new DimTradeOrder(orderid, orderNo, userId, status, totalMoney, areaId);
            });
            tableEnv.createTemporaryView("lagou_orders",orderStream);
            //通过sql查询
            String sql ="select f.city,f.province,sum(f.qusum) as orderMoney, sum(f.qucount) as orderCount " +
                    "from (select r.aname as qu,r.city as city,r.province as province,sum(k.totalMoney) as qusum,count(k.totalMoney) as qucount " +
                    "from lagou_orders as k inner join dim_lagou_area as r on k.areaId = r.areaId group by r.aname,r.city,r.province) as f " +
                    "group by f.city,f.province";

            Table resultTable = tableEnv.sqlQuery(sql);
            DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(resultTable, Row.class);
            result.filter(x->x.f0 == true).print();

            //4.运行
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
