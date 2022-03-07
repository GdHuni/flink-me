package real_dw.util;


import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Calendar;

public class CodisUtil {
//	private static Logger logger = Logger.getLogger(CodisUtil.class);
//	private static JedisResourcePool jedisPool;
//	public static void init() {
//		if (null == jedisPool) {
//			JedisPoolConfig config = new JedisPoolConfig();
//			config.setMaxTotal(500);
//	        config.setMaxIdle(5);
//			config.setMaxWaitMillis(30000);
//	        config.setTestOnBorrow(true);
//			jedisPool = RoundRobinJedisPool.create()
//			        .curatorClient(PropertiesUtil.readValue("codis.zookeeper.addr"), 30000)
//			        .zkProxyDir(PropertiesUtil.readValue("codis.proxy.addr"))
//			        .poolConfig(config).build();
//		}
//	}
//
//	public static Jedis getClient() {
//		init();
//        Jedis jedis = jedisPool.getResource();
//
//        return jedis;
//	}

	public static long getValidTime(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_YEAR, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return (cal.getTimeInMillis() - System.currentTimeMillis()) / 1000;
	}

//	public static void main(String[] args) {
//		Jedis jedis = null;
//		try {
//			String userKey = "122d521bcf0d07d375ca4e54a97062b";
//			jedis = CodisUtil.getClient();
//			//jedis.set(userKey, text, "NX", "EX", 60*60*1);
//			String redisStr = jedis.get(userKey);
//			System.out.println(redisStr);
//			jedis = CodisUtil.getClient();
//		} catch (Exception e){
//			logger.error(e);
//		} finally {
//			if(null != jedis) {
//				jedis.close();
//			}
//		}
//	}
}
