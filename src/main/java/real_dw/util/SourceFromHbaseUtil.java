package real_dw.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;


/**
 * @Classname SourceFromHbaseUtil
 * @Description  从hbase获取数据工具类
 * @Date 2022/1/29 16:21
 * @author huni
 */
public class SourceFromHbaseUtil extends RichSourceFunction<Tuple2<String,String>> {

    private Connection connection = null;
    private Table table = null;
    private Scan scan = null;
    boolean flag = false;
    /**
     * hbase表名
     */
    private String tableName;

    public SourceFromHbaseUtil(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
     
        connection = HbaseUtil.getHbaseConnection("linux121,linux122,linux123");
        String cf1  = "info";
        table = connection.getTable(TableName.valueOf(tableName));
        scan = new Scan();
        scan.addFamily(Bytes.toBytes(cf1));
    }


    @Override
    public void run(SourceContext<Tuple2<String,String>> ctx) throws Exception {
        if(!flag) {
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while(iterator.hasNext()){
                Result result = iterator.next();
                String row = Bytes.toString(result.getRow());
                StringBuffer sb = new StringBuffer();
                for (Cell cell : result.listCells()) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    sb.append(value).append("#-#");
                }
                String valueString = sb.replace(sb.length() - 3, sb.length(), "").toString();
                ctx.collect(new Tuple2<>(row,valueString));
            }
        }
    }

    @Override
    public void cancel() {
        flag = true;
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

}
