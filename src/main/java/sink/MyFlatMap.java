package sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import util.JsonUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lenovo
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.lyj.flink.operator
 * @创建作者: dengq@leyoujia.com
 * @创建日期: 2021/5/18 10:26
 */
public class MyFlatMap implements FlatMapFunction<String, Tuple3<String,String,Integer>> {


    private static final String TYPE_1 = "INSERT";
    private static final String TYPE_2 = "UPDATE";


    @Override
    public void flatMap(String log, Collector<Tuple3<String,String,Integer>> collector) throws Exception {
        if (StringUtils.isBlank(log)) {
            return;
        }
        System.out.println("原始日志为：" +  log);
        String type = JsonUtil.getJsonValueByKey(log, "type");
        String data = JsonUtil.getJsonValueByKey(log, "data");
        String old = JsonUtil.getJsonValueByKey(log, "old");
        String other2 = JsonUtil.getArrayValueByKey(data, "other2");
        String workerId = JsonUtil.getArrayValueByKey(data, "worker_id");
        String idPath = JsonUtil.getArrayValueByKey(data, "id_path");
        String startTime = JsonUtil.getArrayValueByKey(data, "start_time");
        String endTime = JsonUtil.getArrayValueByKey(data, "end_time");
        String oldOther2 = JsonUtil.getArrayValueByKey(old, "other2");
        String key = idPath + "-" + workerId;



        if (!startTime.isEmpty() && endTime.isEmpty()) {

            if (type.equals(TYPE_1)) {
                String[] split = other2.split(",");
                for (String s : split) {
                    if (!"1".equals(s)) {

                        if("13".equals(s) || "17".equals(s) || "21".equals(s)){
                            collector.collect(new Tuple3<>(key,s,1));
                        }else{
                            collector.collect(new Tuple3<>(key, s, 1));
                        }
                    }
                }
            } else if (type.equals(TYPE_2)) {

                if (!oldOther2.isEmpty() && !JsonUtil.isExistsByJsonArrayKey(log,"old" , "start_time")  ) {

                    String[] split = other2.split(",");
                    String[] oldSplit = oldOther2.split(",");
                    Set<String> splitSet = new HashSet<>();
                    Set<String> oldSplitSet = new HashSet<>();
                    for (String s : split) {
                        if ( !"1".equals(s)) {
                            splitSet.add(s);

                            if("13".equals(s) || "17".equals(s) || "21".equals(s)){
                                collector.collect(new Tuple3<>(key,s,1));
                            }else {
                                collector.collect(new Tuple3<>(key,other2,1 ));
                            }
                        }
                    }
                    for (String s : oldSplit) {
                        if ( !"1".equals(s)) {
                            oldSplitSet.add(s);

                            if("13".equals(s) || "17".equals(s) || "21".equals(s)){
                                collector.collect(new Tuple3<>(key,s,-1));
                               // collector.collect(new Tuple3<>(key,s ,-1));
                            }else if("1".equals(s)){
                                collector.collect(new Tuple3<>(key,s,0));
                            }else {
                                collector.collect(new Tuple3<>(key,s,-1));                            }
                        }
                    }

                }else if (JsonUtil.isExistsByJsonArrayKey(log,"old" , "start_time")){
                    String[] split = other2.split(",");
                    Set<String> splitSet = new HashSet<>();
                    for (String s : split) {
                        if ( !"1".equals(s)) {
                            splitSet.add(s);
                            if("13".equals(s) || "17".equals(s) || s.equals(s)){
                                collector.collect(new Tuple3<>(key,s,1));
                              //  collector.collect(new Tuple3<>(key,s ,splitSet.size() ));
                            }else{
                                collector.collect(new Tuple3<>(key,s ,splitSet.size() ));
                            }
                        }
                    }
                }
            }

        } else if (endTime.contains("20")) {

            String[] split = other2.split(",");
            //Set<String> splitSet = new HashSet<>();
            for (String s : split) {
                if (!"1".equals(s)) {

                    if("13".equals(s) || "17".equals(s) || "21".equals(s)){
                        collector.collect(new Tuple3<>(key,s,-1));
                    }else {
                        collector.collect(new Tuple3<>(key,s,-1));
                     }
                }
            }

        }
    }

}