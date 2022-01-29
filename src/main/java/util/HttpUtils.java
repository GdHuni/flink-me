package util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @功能描述:
 * @项目版本: 1.0.0
 * @项目名称: 大数据服务平台 - util
 * @相对路径: com.leyoujia.bds.common.utils.HttpUtils
 * @author: <a href="mailto:tdf@leyoujia.com">谭当飞</a>
 * @创建日期: 2021/1/11 0011 17:43
 */

public class HttpUtils {

    /**
     * unicode转中文
     *
     * @Data:下午1:44:16
     * @Package:testhttpclient.testhttpclient
     * @Return:String
     * @Auth:diao
     */
    public static String decodeUnicode(final String dataStr) {
        int start = 0;
        int end = 0;
        final StringBuffer buffer = new StringBuffer();
        while (start > -1) {
            end = dataStr.indexOf("\\u", start + 2);
            String charStr = "";
            if (end == -1) {
                charStr = dataStr.substring(start + 2, dataStr.length());
            } else {
                charStr = dataStr.substring(start + 2, end);
            }
            // 16进制parse整形字符串。
            char letter = (char) Integer.parseInt(charStr, 16);
            buffer.append(new Character(letter).toString());
            start = end;
        }
        return buffer.toString();
    }


    public static RequestConfig setConfig(String address, int port){
        RequestConfig.Builder builder = RequestConfig.custom().setConnectionRequestTimeout(10 * 1000).setSocketTimeout(10 * 1000);

        if(StringUtils.isNotEmpty(address)){
            HttpHost proxy = new HttpHost(address, port);
            builder.setProxy(proxy);
        }

        return builder.build();
    }

    public static RequestConfig setConfig(){
        RequestConfig.Builder builder = RequestConfig.custom().setConnectionRequestTimeout(10 * 1000).setSocketTimeout(10 * 1000);
        return builder.build();
    }


    /**
     * get请求传递的连接，请求头
     *
     * @Data:上午11:15:10
     * @Package:testhttpclient.testhttpclient
     * @Return:Map<String,String>
     * @Auth:diao
     */
    public static Map<String, Object> get(String url, Map<String, String> headers,Set<String> cookies) {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setConfig(setConfig());
        headers.forEach((a, b) -> {
            httpGet.addHeader(a, b);
        });

        if(null != cookies){
            httpGet.addHeader("cookie", cookies.toString().substring(1, cookies.toString().length() - 1));
        }

        CloseableHttpClient httpclient = HttpClients.createDefault();

        CloseableHttpResponse respose = null;

        try {
            respose = httpclient.execute(httpGet);
        } catch (ClientProtocolException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    httpclient = null;
                }
            }
        }
        return getResData(respose, httpGet);
    }

    public static Map<String, Object> post(String url) throws UnsupportedEncodingException {
        return post(url,null,null,"utf-8",null);
    }

    public static Map<String, Object> post(String url,Map<String, String> params) throws UnsupportedEncodingException {
        return post(url,null,params,"utf-8",null);
    }

    /**
     * post请求方式
     *
     * @Data:下午2:01:15
     * @Package:testhttpclient.testhttpclient
     * @Return:Map<String,String>
     * @Auth:diao
     */
    public static Map<String, Object> post(String url, Map<String, String> headers, Map<String, String> params, String charset,Set<String> cookies) throws UnsupportedEncodingException {
        //设置请求方法及连接信息
        HttpPost httpPost = new HttpPost(url);
        //设置请求参数
        httpPost.setConfig(setConfig());
        if(null != headers){
            //设置请求头
            headers.forEach((a, b) -> {
                httpPost.addHeader(a, b);
            });
        }


        if(null != cookies){
            BasicCookieStore cookie = new BasicCookieStore();
            HttpClients.custom().setDefaultCookieStore(cookie).build();
            httpPost.addHeader("cookie", cookies.toString().substring(1, cookies.toString().length() - 1));
        }

        List<NameValuePair> pairs = null;
        if (params != null && !params.isEmpty()) {
            pairs = new ArrayList<NameValuePair>(params.size());
            for (String key : params.keySet()) {
                pairs.add(new BasicNameValuePair(key, params.get(key).toString()));
            }
        }
        if (pairs != null && pairs.size() > 0) {
            httpPost.setEntity(new UrlEncodedFormEntity(pairs, charset));
        }
        //执行请求
        HttpResponse response = null;
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            response = httpclient.execute(httpPost);
        } catch (ClientProtocolException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {

            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            if (httpclient != null) {
                try {
                    httpclient.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    httpclient = null;
                }
            }
        }
        return getResData(response, httpPost);
    }


    public static String postJson(String url, Map<String, Object> params){

        String body = "";

        //创建httpclient对象
        CloseableHttpClient client = HttpClients.createDefault();
        //创建post方式请求对象
        HttpPost httpPost = new HttpPost(url);

        //装填参数
        StringEntity s = new StringEntity(JSON.toJSONString(params), "utf-8");
        s.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
        //设置参数到请求对象中
        httpPost.setEntity(s);

        //设置header信息
        //指定报文头【Content-type】、【User-Agent】
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");

        CloseableHttpResponse response = null;

        try {
            //执行请求操作，并拿到结果（同步阻塞）
            response = client.execute(httpPost);
            //获取结果实体
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                //按指定编码转换结果实体为String类型
                body = EntityUtils.toString(entity, "utf-8");
            }
            EntityUtils.consume(entity);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //释放链接
            try {
                if(null != response){
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return body;
    }

    public static String postJsonStr(String url, String params){

        String body = "";

        //创建httpclient对象
        CloseableHttpClient client = HttpClients.createDefault();
        //创建post方式请求对象
        HttpPost httpPost = new HttpPost(url);

        //装填参数
        StringEntity s = new StringEntity(params, "utf-8");
        s.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "text/html"));
        //设置参数到请求对象中
        httpPost.setEntity(s);

        //设置header信息
        //指定报文头【Content-type】、【User-Agent】
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setHeader("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");

        CloseableHttpResponse response = null;

        try {
            //执行请求操作，并拿到结果（同步阻塞）
            response = client.execute(httpPost);
            //获取结果实体
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                //按指定编码转换结果实体为String类型
                body = EntityUtils.toString(entity, "utf-8");
            }
            EntityUtils.consume(entity);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //释放链接
            try {
                if(null != response){
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return body;
    }

    /**
     * http post请求
     * @param url 请求地址
     * @param jsonString json格式的参数
     * @return
     */
    public static String postRequest(String url, String jsonString) {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        String result = null;
        try {
            StringEntity s = new StringEntity(jsonString,"utf-8");
            s.setContentEncoding("UTF-8");
            //发送json数据需要设置contentType
            s.setContentType("application/json");
            post.setEntity(s);

            HttpResponse res = client.execute(post);
            if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                // 返回json格式：
                result = EntityUtils.toString(res.getEntity());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
            }
        }
        return result;
    }

    /**
     * http get请求
     * @param url 请求地址
     * @return
     */
    public static String getRequest(String url) {
        String result = "";
        BufferedReader in = null;
        try {

            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent","Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;

    }

    /**
     * 处理http结果请求
     *
     * @Data:下午1:42:28
     * @Package:testhttpclient.testhttpclient
     * @Return:Map<String,String>
     * @Auth:diao
     */
    public static Map<String, Object> getResData(HttpResponse response, HttpRequestBase requestbase) {
        int status = 405;

        Map<String,Object> resultMap = new HashMap<>(16);
        resultMap.put("status", status);
        resultMap.put("result", null);
        if (status != 200) {
            requestbase.abort();
        } else {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try {
                    String data = EntityUtils.toString(entity, "utf-8");

                    String start = "[", end = "]";
                    if (data.startsWith(start)) {
                        data = data.substring(1);
                    }
                    if (data.endsWith(end)) {
                        data = data.substring(0, data.length() - 1);
                    }
                    data = data.replaceAll("\\\\t|\\\\n|\\r\\n", "");
                    data = data.replaceAll("\\\\/", "/");
                    data = decodeUnicode(data);
                    resultMap.put("result", data);
                    //关闭流
                    EntityUtils.consume(entity);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                response = null;
            }
        }
        return resultMap;
    }

    /**
     * 获取请求头
     *
     * @Data:下午1:44:16
     * @Package:testhttpclient.testhttpclient
     * @Return:String
     * @Auth:diao
     */
    public Map<String, String> getCommHeader() {
        Map<String, String> headers = new HashMap<String, String>(16);
        headers.put("User-Agent", "Mozilla/5.0(Window NT 6.1; WOW64; rv:58.0) Gecko/20100101 Firfox/58.0");
        headers.put("Accept", "application/json,text/plain,*/*");
        headers.put("Accept-Language", "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2");
        headers.put("Accept-Encoding", "gzip,deflate");
        headers.put("Connection", "keep-alive");
        return headers;
    }



}