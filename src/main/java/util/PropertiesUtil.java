package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * @author lenovo
 */
public class PropertiesUtil {
        public static Properties confProperties;
        static {
            try {
                init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static void init() throws IOException {
            if (confProperties == null) {
                confProperties = new Properties();
                InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream("product/config.properties");
                try {
                    confProperties.load(in);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if(null != in){
                        in.close();
                    }

                }
            }
        }

        public static Properties getProperties() throws IOException {
            init();
            return confProperties;
        }

        public static void clear() {
            confProperties.clear();
            confProperties = null;
        }

        public static String get(String key) {
            try {
                init();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return confProperties.getProperty(key);
        }
}