package util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import java.util.List;
import java.util.Map;

/**
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.lyj.util
 * @创建作者: dengq@leyoujia.com
 * @创建日期: 2021/5/18 09:53
 */
public class JsonUtil {
    /**
     * 功能描述：把JSON数据转换成指定的java对象
     *
     * @param jsonData JSON数据
     * @param clazz    指定的java对象
     * @return 指定的java对象
     */
    public static <T> T getJsonToBean(String jsonData, Class<T> clazz) {
        return JSON.parseObject(jsonData, clazz);
    }

    /**
     * 功能描述：把java对象转换成JSON数据
     *
     * @param object java对象
     * @return JSON数据
     */
    public static String getBeanToJson(Object object) {
        return JSON.toJSONString(object);
    }

    /**
     * 功能描述：把JSON数据转换成指定的java对象列表
     *
     * @param jsonData JSON数据
     * @param clazz    指定的java对象
     * @return List<T>
     */
    public static <T> List<T> getJsonToList(String jsonData, Class<T> clazz) {
        return JSON.parseArray(jsonData, clazz);
    }

    /**
     * 功能描述：把JSON数据转换成较为复杂的List<Map<String, Object>>
     *
     * @param jsonData JSON数据
     * @return List<Map < String, Object>>
     */
    public static List<Map<String, Object>> getJsonToListMap(String jsonData) {
        return JSON.parseObject(jsonData, new TypeReference<List<Map<String, Object>>>() {
        });
    }

    /**
     * List<T> 转 json 保存到数据库
     */
    public static <T> String listToJson(List<T> ts) {
        String jsons = JSON.toJSONString(ts);
        return jsons;
    }

    /**
     * 两个类之间值的转换
     * 从object》》tClass
     *
     * @param object 有数据的目标类
     * @param tClass 转换成的类
     * @param <T>
     * @return 返回tClass类
     */
    public static <T> T getObjectToClass(Object object, Class<T> tClass) {
        String json = getBeanToJson(object);
        return getJsonToBean(json, tClass);
    }

    /**
     * json 转 List<T>
     */
    public static <T> List<T> jsonToList(String jsonString, Class<T> clazz) {
        @SuppressWarnings("unchecked")
        List<T> ts = JSONArray.parseArray(jsonString, clazz);
        return ts;
    }

    /**
     * 获取json串的值根据key
     * @param json
     * @param key
     * @return
     */

    public static String getJsonValueByKey(String json,String key){

        JSONObject jsonObject = JSON.parseObject(json);

        return jsonObject.getString(key);
    }

    /**
     * 获取json数组中第一个json串的值
     * @param jsonArray
     * @param key
     * @return
     */
    public static String getArrayValueByKey(String jsonArray,String key){

        try {
            if (null == jsonArray){
                return "";
            }
            JSONArray JsonArray  =  JSONArray.parseArray(jsonArray);

            String string = JsonArray.getJSONObject(0).getString(key);
            if(null != string){
                return  string;
            }else {
                return "";
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static boolean isExistsByJsonArrayKey(String json,String JsonArrayKey,String key){
        JSONObject jsonObject = JSON.parseObject(json);
        JSONArray jsonArray = jsonObject.getJSONArray(JsonArrayKey);
        JSONObject jsonObject1 = jsonArray.getJSONObject(0);
        boolean flag = jsonObject1.containsKey(key);
        return flag;
    }

    public static void main(String[] args) {
        String str = "{\"data\":[{\"id\":\"995138692\",\"worker_id\":\"01000098\",\"worker_name\":\"周棵\",\"memotext\":\"与同事@同事一同招聘\",\"position\":null,\"createTime\":\"2021-05-19 20:55:24\",\"other1\":null,\"other2\":\"22\",\"source\":\"2\",\"manager_number\":\"88888888\",\"manager_name\":\"林凤辉\",\"dept_number\":\"5501460\",\"dept_name\":\"家家顺物联科技\",\"duty_number\":\"7700547\",\"duty_name\":\"总经理(K9)\",\"start_time\":\"2021-05-19 20:55:45\",\"end_time\":null,\"parent_id\":null,\"sta_lat\":null,\"end_lat\":null,\"sta_lon\":null,\"end_lon\":null,\"add_imei\":\"\",\"sta_imei\":null,\"end_imei\":null,\"BI_DATE\":\"2021-05-19 20:55:45\",\"id_path\":\"88888888/01000098/\",\"check_status\":\"9\",\"check_number\":null,\"check_name\":null,\"check_time\":null}],\"database\":\"coa\",\"es\":1621428945000,\"id\":3162,\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"worker_id\":\"varchar(10)\",\"worker_name\":\"varchar(50)\",\"memotext\":\"text\",\"position\":\"varchar(100)\",\"createTime\":\"datetime\",\"other1\":\"varchar(200)\",\"other2\":\"varchar(200)\",\"source\":\"varchar(2)\",\"manager_number\":\"varchar(11)\",\"manager_name\":\"varchar(50)\",\"dept_number\":\"varchar(10)\",\"dept_name\":\"varchar(100)\",\"duty_number\":\"varchar(10)\",\"duty_name\":\"varchar(50)\",\"start_time\":\"datetime\",\"end_time\":\"datetime\",\"parent_id\":\"int(11)\",\"sta_lat\":\"varchar(50)\",\"end_lat\":\"varchar(50)\",\"sta_lon\":\"varchar(50)\",\"end_lon\":\"varchar(50)\",\"add_imei\":\"varchar(100)\",\"sta_imei\":\"varchar(100)\",\"end_imei\":\"varchar(100)\",\"BI_DATE\":\"timestamp\",\"id_path\":\"varchar(128)\",\"check_status\":\"int(11)\",\"check_number\":\"varchar(32)\",\"check_name\":\"varchar(32)\",\"check_time\":\"timestamp\"},\"old\":[{\"start_time\":null,\"BI_DATE\":\"2021-05-19 20:55:29\"}],\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":4,\"worker_id\":12,\"worker_name\":12,\"memotext\":2005,\"position\":12,\"createTime\":93,\"other1\":12,\"other2\":12,\"source\":12,\"manager_number\":12,\"manager_name\":12,\"dept_number\":12,\"dept_name\":12,\"duty_number\":12,\"duty_name\":12,\"start_time\":93,\"end_time\":93,\"parent_id\":4,\"sta_lat\":12,\"end_lat\":12,\"sta_lon\":12,\"end_lon\":12,\"add_imei\":12,\"sta_imei\":12,\"end_imei\":12,\"BI_DATE\":93,\"id_path\":12,\"check_status\":4,\"check_number\":12,\"check_name\":12,\"check_time\":93},\"table\":\"app_mobileMemo\",\"ts\":1621428945464,\"type\":\"UPDATE\"}";
        //System.out.println(getArrayValueByKey(getJsonValueByKey(str,"old" ),"start_time"));
        System.out.println(isExistsByJsonArrayKey(str,"old" ,"start_time"));
    }

}
