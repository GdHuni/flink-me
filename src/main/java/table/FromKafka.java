package table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * FlinkTable从kafka上获取数据
 */
public class FromKafka {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //tEnv
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
/*        String s = "CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 指定连接类型是kafka\n" +
                "    'connector.version' = '0.11',  -- 与我们之前Docker安装的kafka版本要一致\n" +
                "    'connector.topic' = 'mykafka', -- 之前创建的topic \n" +
                "    'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度\n" +
                "    'connector.startup-mode' = 'earliest-offset',  --指定从最早消费\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zk地址\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址\n" +
                "    'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致\n" +
                ")";*/
        String s = "create table HSL_QUANTIZATION (\n" +
                "    data ARRAY<ROW<ID STRING,TYPE STRING,FH_ID STRING,ENTRUST_ID BIGINT,RS_TYPE STRING,WORKER_ID STRING,DEPT_NUMBER STRING,DETAIL_CONTENT STRING,SCORE BIGINT,CREATE_ID STRING,CREATE_DATE STRING,UPDATE_ID STRING,UPDATE_DATE STRING,BI_DATE STRING>>,\n" +
                "    database  STRING,\n" +
                "    es  BIGINT,\n" +
                "    id  BIGINT,\n" +
                "    isDdl  BOOLEAN,\n" +
                "    mysqlType  ROW<ID STRING,TYPE STRING,FH_ID STRING,ENTRUST_ID BIGINT,RS_TYPE STRING,WORKER_ID STRING,DEPT_NUMBER STRING,DETAIL_CONTENT STRING,SCORE BIGINT,CREATE_ID STRING,CREATE_DATE STRING,UPDATE_ID STRING,UPDATE_DATE STRING,BI_DATE STRING>,\n" +
                "    `old`  ARRAY<ROW<ID STRING,TYPE STRING,FH_ID STRING,ENTRUST_ID BIGINT,RS_TYPE STRING,WORKER_ID STRING,DEPT_NUMBER STRING,DETAIL_CONTENT STRING,SCORE BIGINT,CREATE_ID STRING,CREATE_DATE STRING,UPDATE_ID STRING,UPDATE_DATE STRING,BI_DATE STRING>>,\n" +
                "    pkNames  ARRAY<STRING>,\n" +
                "    `sql`  STRING,\n" +
                "    `sqlType`    ROW<ID BIGINT,TYPE BIGINT,FH_ID BIGINT,ENTRUST_ID BIGINT,RS_TYPE BIGINT,WORKER_ID BIGINT,DEPT_NUMBER BIGINT,DETAIL_CONTENT BIGINT,SCORE BIGINT,CREATE_ID BIGINT,CREATE_DATE BIGINT,UPDATE_ID BIGINT,UPDATE_DATE BIGINT,BI_DATE BIGINT>,\n" +
                "    `table`  STRING,\n" +
                "    ts  BIGINT,\n" +
                "    `type`  STRING\n" +
                ")\n" +
                "with  (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'huni_topic',\n" +
                "    'properties.bootstrap.servers' = '172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093',\n" +
                "    'properties.group.id' = 'huni_test',\n" +
                "    'scan.startup.mode' = 'group-offsets',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true',\n" +
                "    'properties.zookeeper.connect' = '172.16.21.200:2181/kafka'\n" +
                ")";
        tEnv.sqlUpdate(s);

/*        tEnv.connect(
                new Kafka()
                .version("universal")
                .topic("huni_topic")
                .startFromEarliest()
                .property("bootstrap.servers","172.16.21.200:9093,172.16.5.29:9093,172.16.5.30:9093")
        )
                .withFormat(new Csv())
                .withSchema(
                        new Schema().field("name", DataTypes.STRING())
                )
                .createTemporaryTable("HSL_QUANTIZATION");*/

        String sql = "select\n" +
                "a.data[1].ID,\n" +
                "a.data[1].WORKER_ID,\n" +
                "a.data[1].CREATE_DATE,\n" +
                "a.data[1].FH_ID,\n" +
                "a.data[1].RS_TYPE,\n" +
                "a.data[1].DETAIL_CONTENT,\n" +
                "a.data[1].`TYPE`,\n" +
                "TO_DATE(data[1].CREATE_DATE)\n" +
                "from HSL_QUANTIZATION a \n";
        Table resultTable = tEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();
        env.execute();

    }
}
