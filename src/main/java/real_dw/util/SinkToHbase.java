package real_dw.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import real_dw.entity.HbaseTradeOrdersVo;
import real_dw.entity.TradeOrdersVo;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author huni
 * @Classname SinkToHbase
 * @Description TODO
 * @Date 2022/1/30 10:20
 */
public class SinkToHbase extends RichSinkFunction<HbaseTradeOrdersVo> {

    private Connection connection = null;
    private Table table = null;
    private ArrayList<Put> putArrayList;
    private BufferedMutatorParams params;
    private BufferedMutator mutator;


    private String cf = "info";
    private String tableName = "trade_orders";

    @Override
    public void open(Configuration parameters) {
        try {
            connection = HbaseUtil.getHbaseConnection("linux121,linux122,linux123");
            table = connection.getTable(TableName.valueOf(tableName));
            /*params = new BufferedMutatorParams(TableName.valueOf(tableName));
            params.writeBufferSize(1024 * 1024);
            mutator = connection.getBufferedMutator(params);
            putArrayList = new ArrayList<>();*/
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
    public void invoke(HbaseTradeOrdersVo value, Context context) {
        String type = value.getType();
        String dateBaseName = value.getDateBaseName();
        String tableName = value.getTableName();

        //获取data数据信息
        JSONArray datas = JSONArray.parseArray(value.getDataInfo());
        //订单表数据
        if(dateBaseName.equalsIgnoreCase("dwads") && tableName.equalsIgnoreCase("lagou_trade_orders")) {
            if(type.equalsIgnoreCase("insert")) {
                for (Object data : datas) {
                    TradeOrdersVo tradeOrdersVo = JSON.toJavaObject(JSONObject.parseObject(data.toString()), TradeOrdersVo.class);
                    insertTradeOrders(table,tradeOrdersVo);
                }
            }
        }


    }
    public void insertTradeOrders(Table table, TradeOrdersVo tradeOrdersVo){
        try {
            //创建put对象
            Put put = new Put(Bytes.toBytes(tradeOrdersVo.getOrderId()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("orderNo"),Bytes.toBytes(tradeOrdersVo.getOrderNo()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("isPay"),Bytes.toBytes(tradeOrdersVo.getIsPay()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("orderId"),Bytes.toBytes(tradeOrdersVo.getOrderId()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tradeSrc"),Bytes.toBytes(tradeOrdersVo.getTradeSrc()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("payTime"),Bytes.toBytes(tradeOrdersVo.getPayTime()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("productMoney"),Bytes.toBytes(tradeOrdersVo.getProductMoney()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("totalMoney"),Bytes.toBytes(tradeOrdersVo.getTotalMoney()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("dataFlag"),Bytes.toBytes(tradeOrdersVo.getDataFlag()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("userId"),Bytes.toBytes(tradeOrdersVo.getUserId()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("areaId"),Bytes.toBytes(tradeOrdersVo.getAreaId()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("createTime"),Bytes.toBytes(tradeOrdersVo.getCreateTime()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("payMethod"),Bytes.toBytes(tradeOrdersVo.getPayMethod()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("isRefund"),Bytes.toBytes(tradeOrdersVo.getIsRefund()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("tradeType"),Bytes.toBytes(tradeOrdersVo.getTradeType()));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("status"),Bytes.toBytes(tradeOrdersVo.getStatus()));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
