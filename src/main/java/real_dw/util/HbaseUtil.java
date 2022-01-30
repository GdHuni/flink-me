package real_dw.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_OPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_CLIENT_PORT;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM;

/**
 * @author huni
 * @Classname HbaseUtil
 * @Description TODO
 * @Date 2022/1/30 10:36
 */
public class HbaseUtil {
    private static Connection connection  = null;

    /**
     * hbase配置信息，获取hbase链接
     * @param zookeeperQuorum
     * @return
     */
    public static Connection getHbaseConnection(String zookeeperQuorum){
       try {

           Configuration conf = HBaseConfiguration.create();
           conf.set(ZOOKEEPER_QUORUM,zookeeperQuorum);
           conf.set(ZOOKEEPER_CLIENT_PORT,"2181");
           conf.setInt(HBASE_CLIENT_OPERATION_TIMEOUT,30000);
           conf.setInt(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,30000);
           connection = ConnectionFactory.createConnection(conf);
       } catch (IOException e) {
           e.printStackTrace();
       }
        return connection;
   }
}
