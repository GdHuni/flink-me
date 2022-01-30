package sink;



import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.DateUtil;
import util.PropertiesUtil;


/**
 * @author lenovo
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.lyj.flink.sink
 * @创建作者: dengq@leyoujia.com
 * @创建日期: 2021/5/17 11:33
 */
public class YxdpRedisSink extends RichSinkFunction<Tuple3<String,String,Integer>> {
    public static Jedis jedis;
    public static DateUtil dateUtil = new DateUtil();



    public static Jedis cliPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        String host = propertiesUtil.get("YxdpBwNum.Redis.Host");
        int port = Integer.parseInt(propertiesUtil.get("YxdpBwNum.Redis.Port"));
        int indexDatabase = Integer.parseInt(propertiesUtil.get("YxdpBwNum.Redis.Database"));
        // 最大连接数
        config.setMaxTotal(10);
        // 最大连接空闲数
        config.setMaxIdle(2);
        // 设置最大阻塞时间，记住是毫秒数milliseconds
        config.setMaxWaitMillis(Long.parseLong("2"));
        JedisPool jedisPool = new JedisPool(config, host, port,2000,null,indexDatabase);

        try {

            return jedisPool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 判断这个key是否存在
     *
     * @param key
     * @return
     */
    public static boolean isExistsKey(String key){
        return jedis.exists(key);
    }


    public static void valueAddOrSubtract(String key,Integer getAddOrSubtract) {

        long ttl = dateUtil.dateSubtraction(dateUtil.getCurrentTime(), dateUtil.getBeforeDay(1));

                if (isExistsKey(key)) {
                    String type = jedis.type(key);
                    if (type.equals("string")) {
                        String oldValue = jedis.get(key);
                        Integer newValue = Integer.parseInt(oldValue) + getAddOrSubtract;
                        if (newValue <= 0) {
                            jedis.del(key);
                        } else {
                            jedis.set(key, newValue.toString());
                            jedis.expire(key, (int) ttl);
                        }
                    }
                } else if (!isExistsKey(key) && getAddOrSubtract > 0 ) {
                            jedis.set(key, "1");
                            jedis.expire(key,(int) ttl);

                }
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = cliPool();

    }

    /**
     * 真实调用的方法
     * @param log
     * @param context
     * @throws Exception
     */

    @Override
    public void invoke(Tuple3<String,String,Integer> log, Context context) throws Exception {
        //因为营销大屏规定只统计在岗经纪人的数据，首先判断这条记录是否为在岗的经纪人的备忘
        String zgjjrKey = "I-ZGJJR-" + log.f0;
        String dqxdKey = null;
        String key = null;
        boolean existsKey = isExistsKey(zgjjrKey);
        if ( existsKey ){
            String bwType = log.f1;
            Integer getAddOrSubtract = log.f2;
            if(bwType.equals("13")){
                 key = "I-BT-" + log.f0;
                valueAddOrSubtract(key,getAddOrSubtract);
            }else if (bwType.equals("17")){
                 key = "I-ST-" + log.f0;
                valueAddOrSubtract(key,getAddOrSubtract);
            }else if (bwType.equals("21")){
                 key = "I-MF-" + log.f0;
                valueAddOrSubtract(key,getAddOrSubtract);
            }
            dqxdKey = "I-DQXD-" +log.f0;
            valueAddOrSubtract(dqxdKey,getAddOrSubtract);

        }

    }

    /**
     * 关闭redis连接
     * @throws Exception
     */

    @Override
    public void close() throws Exception {

        super.close();
        jedis.close();
    }

    public static void main(String[] args) {
        String dengq = cliPool().get("dengq");
        System.out.println(dengq);
    }
}
