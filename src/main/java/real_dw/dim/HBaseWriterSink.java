package real_dw.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import real_dw.entity.TradeOrdersVo;
import real_dw.util.HbaseUtil;

import java.io.IOException;

/**
 * @author huni
 * @Classname SinkToHbase
 * @Description sink到hbase的工具类
 * @Date 2022/1/30 10:20
 */
public class HBaseWriterSink extends RichSinkFunction<String> {

    private Connection connection = null;
    private Table table = null;


    @Override
    public void open(Configuration parameters) {
        try {
            String tableName = "dim_area";
            connection = HbaseUtil.getHbaseConnection("linux121,linux122,linux123");
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(String value, Context context) {
        insertDimArea(table,value);
    }

    /**
     * 订单信息插入hbase
     *
     * @param table    hbase 表对象
     * @param value 值
     */
    public void insertDimArea(Table table, String value) {
        try {

            String[] infos = value.split(",");
            String areaId  = infos[0].trim();
            String aname  = infos[1].trim();
            String cid  = infos[2].trim();
            String city  = infos[3].trim();
            String proid  = infos[4].trim();
            String province  = infos[5].trim();
            //创建put对象
            Put put = new Put(Bytes.toBytes(areaId));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("aname"), Bytes.toBytes(aname));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cid"), Bytes.toBytes(cid));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(city));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("proid"), Bytes.toBytes(proid));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("province"), Bytes.toBytes(province));

            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * hbase根据id删除订单信息
     *
     * @param table    hbase 表对象
     * @param tradeOrdersVo 订单信息实体类
     */
    public void deleteTradeOrders(Table table, TradeOrdersVo tradeOrdersVo) {
        try {
            Delete delete = new Delete(tradeOrdersVo.getOrderId().getBytes());
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
