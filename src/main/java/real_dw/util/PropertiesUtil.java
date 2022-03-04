package real_dw.util;

import java.io.IOException;
import java.util.Properties;

/**
 * @功能描述: 参数文件
 */
public class PropertiesUtil {
	private static Properties prop = new Properties(); 
    static { 
        try { 
            prop.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("product/bigdata-config.properties"));
            System.out.println();
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
   
    /**  
     * @功能描述: 获取值
     * @param key
     * @return
     */ 
    public static String readValue(String key) { 
        return (String) prop.get(key); 
    } 
}
