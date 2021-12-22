package sink.custom_sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * flink to ch
 * 报错可以参考
 * https://blog.csdn.net/zh17673640696/article/details/121721484
 */
public class MySinkToClickHouse {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ChBean b1 = new ChBean(System.currentTimeMillis(), "huni", 20);
        ChBean b2 = new ChBean(System.currentTimeMillis(), "huni1", 21);
        ChBean b3 = new ChBean(System.currentTimeMillis(), "huni2", 22);
        ChBean b4 = new ChBean(System.currentTimeMillis(), "huni3", 23);
        ChBean b5 = new ChBean(System.currentTimeMillis(), "huni4", 24);

        DataStreamSource<ChBean> data = env.fromElements(b1,b2,b3,b4,b5);

        //jdbc:clickhouse://192.168.216.2:8123/lyj_ch_dw
        String chDriver ="ru.yandex.clickhouse.ClickHouseDriver";
        Class.forName(chDriver);
        String chUrl = "jdbc:clickhouse://172.16.5.32:8123/tmp";
        data.addSink(new RichSinkFunction<ChBean>() {
            Connection conn;
            PreparedStatement pst;
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection(chUrl);
                String sql = "insert into tmp.jdbc_example (day,name,age) values (?,?,?)";
                pst = conn.prepareStatement(sql);
                //pst.executeQuery("create table tmp.jdbc_example1(day Date, name String, age UInt8) Engine=Log");

            }

            @Override
            public void invoke(ChBean value, Context context) throws Exception {
                for (int i = 0; i < 5; i++) {
                    pst.setDate(1, new Date(value.getDate()));
                    pst.setString(2, value.getName());
                    pst.setInt(3, value.getAge());
                    pst.addBatch();
                }

                pst.executeBatch();
            }

            @Override
            public void close() throws Exception {
                if (conn != null) {
                    conn.close();
                }
                if (pst != null) {
                    pst.close();
                }
            }
        });

        env.execute();

    }
}

class ChBean implements Serializable{
    private long date;
    private String name;
    private int age;

    public ChBean(long date, String name, int age) {
        this.date = date;
        this.name = name;
        this.age = age;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "ChBean{" +
                "date=" + date +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}