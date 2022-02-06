package real_dw.dim;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import real_dw.entity.AreaDetailVo;
import real_dw.util.SourceFromHbaseUtil;

/**
 * @author huni
 * @Classname AreaDim
 * @Description dim层地域信息表
 * @Date 2022/1/30 10:36
 */
public class AreaDim {
    public static void main(String[] args) throws Exception {
        //1.flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<Tuple2<String, String>> data = env.addSource(new SourceFromHbaseUtil("area"));
        data.print();

        SingleOutputStreamOperator<AreaDetailVo> dataStream = data.map(x -> {
            int id = Integer.parseInt(x.f0);
            String[] datas = x.f1.split("#-#");
            String name = datas[5].trim();
            int pid = Integer.parseInt(datas[6].trim());
            return new AreaDetailVo(id, name, pid);
        });
        //转成 地区id,地区的名字，城市的id，城市的名字， 省份的id，省份的名字
        //FlinkTable api
        StreamTableEnvironment tableEnv  = StreamTableEnvironment.create(env);

        //临时表
        tableEnv.createTemporaryView("lagou_area",dataStream);

        //sql -- 生成 区、市、省三级的明细宽表
        String sql  =
                "select a.id as areaid,a.name as aname,a.pid as cid,b.name as city, c.id as proid,c.name as province " +
                        "from lagou_area as a " +
                        "inner join lagou_area as b on a.pid = b.id " +
                        "inner join lagou_area as c on b.pid = c.id ";
        Table areaTable = tableEnv.sqlQuery(sql);

        SingleOutputStreamOperator<String> resultStream = tableEnv.toRetractStream(areaTable, TypeInformation.of(new TypeHint<Row>() {
        })).map(x -> {

            Row row = x.f1;
            String areaId = row.getField(0).toString();
            String aname = row.getField(1).toString();
            String cid = row.getField(2).toString();
            String city = row.getField(3).toString();
            String proid = row.getField(4).toString();
            String province = row.getField(5).toString();
            return areaId + "," + aname + "," + cid + "," + city + "," + proid + "," + province;
        });
        resultStream.print();
        resultStream.addSink(new HBaseWriterSink());
        env.execute();
    }

}
