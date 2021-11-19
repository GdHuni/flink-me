package util;

/**
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.lyj.util
 * @创建作者: dengq@leyoujia.com
 * @创建日期: 2020/5/23 17:01
 */
public class LogSourceUtil {
    static  long i =1;
    public String getWriteRedis( String log){
        System.out.println(i++);
        String SpecialChar_1 = "\u0001";
        String SpecialChar_2 = "\\|\\|";
        String[] PcArrLog = log.split(SpecialChar_1);

        if(PcArrLog.length == 20){
            String loc = PcArrLog[10];
            String uid = PcArrLog[2];
            String sid = PcArrLog[19];
            /**
             * 此处pc/wap未用会话id（sid）原因：pc/wap中的会话id保存时间问题，不是小程序和app那边的90秒就发生变化的
             */
            if(loc.indexOf("test") < 0 && loc.indexOf("coa.leyoujia.com") < 0  && loc.indexOf("spider") < 0 && uid.length() == 36 && sid.length() == 36){
                if(loc.indexOf("wap") >= 0){
                    String result = "LYJ_WAP_LEYOUJIA_" + uid + "=" + uid;
                    return  result;
                }else if (loc.indexOf("bi.leyouji") >= 0 || loc.indexOf("i.leyouji") >= 0 || loc.indexOf("im.leyoujia.com") >= 0){
                    //新系统的数据暂时未写入到redis中去
                    //return "";
                }else{
                    String result = "LYJ_PC_LEYOUJIA_" + uid + "=" + uid;
                    return  result;
                }
            }
        }else {
            String[] ArrLog = log.split(SpecialChar_2);
            //获取aid字段判断出是那个端的数据
            String aid = ArrLog[4];
            String terminal = ArrLog[7];
            if(aid.indexOf("MINI") >= 0){
                //微信特殊埋点的日志
                if(ArrLog.length == 22){
                    /**
                     * 此处因为在nginx中请求头中已经把设备唯一标识符定义为了http_sid，但是在hive表中叫ssid
                     * 固此处现在命名的ssid即为会话id，但是在hive表中叫sid
                     * 针对微信两边是反着的
                     */
                    String ssid = ArrLog[18];
                    String result = "LYJ_" + aid + "_" + ssid + "=" + ssid;
                    return result;
                }else if (ArrLog.length == 16){
                    //微信无埋点的日志
                    String ssid = ArrLog[12];
                    String result = "LYJ_" + aid + "_" + ssid + "=" + ssid ;
                    return result;
                }
            }else if ( aid.indexOf("APP") >= 0 ){
                //app的日志有无埋点的event和特殊埋点的page且长度不一致
                if ( ArrLog.length == 22 ){
                    //这是会话id
                    String sid = ArrLog[19];
                    String result = "LYJ_" + aid + "_" + terminal + "_" + sid + "=" + sid;
                    return result;
                }else if ( ArrLog.length == 23 ){
                    //这是会话id
                    String sid = ArrLog[20];
                    String result = "LYJ_" + aid + "_" + terminal + "_" + sid + "=" + sid;
                    return result;
                }

            }
        }
        return "";
    }

    public static void main(String[] args) {
        LogSourceUtil getLogSourceByLogStyle = new LogSourceUtil();
        String wechat = "2020-06-05 23:59:59||119.123.131.44||0||oVs0Z0ebMAXwLmvBlS-I66yG1BEY||MINI001||-||2.3.1||Android||ELS-AN00||wifi||0||114.007067||22.656852||-||-||-||-||-||745053fd-5803-43c8-baa6-d8a6fb1bcb1b||1036||000002||pId=pages/esfDetail/esfDetail?id=1894346&workId=00333094&st=2020-06-05 23:59:19&et=2020-06-05 23:59:59&bd=HUAWEI&osv=10&scs=363x797&osl=zh_CN&dpi=3.3125&bat=36&uid=";
        String freewechat = "2020-06-05 23:59:59||118.89.83.125||0||oVs0Z0c8tjXr-UVXZGpmGch0bDrM||MINI001||2.3.1||iOS||iPhone6||wifi||0||111.3||30.7||2953700c-ba51-435c-b40f-5576744ae6a9||1129||000002||eId=click&obj={\"pId\":\"pages/esfList/esfList?cityCode=000002\",\"target\":\"#gotoDetail\",\"index\":734}&bd=iPhone&osv=10.0.1&scs=375x667&osl=zh_CN&dpi=1&bat=undefined&uid=";
        String freeapp = "2020-06-05 00:00:01\u0001119.123.120.19\u00012070cdfe-fccf-38ab-853a-91c940ab5025\u0001-\u0001\u0001-\u0001https://shenzhen.leyoujia.com/xq/detail/esf/155602/?s=1\u0001-\u0001Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3314.0 Safari/537.36 SE 2.X MetaSr 1.0\u00011920*1080\u0001https://shenzhen.leyoujia.com/xq/detail/esf/155602/?s=2\u0001null\u0001-\u0001-\u0001JS-V3.1.0\u0001zh-CN\u0001Thu Jun 04 2020 21:30:39 GMT 0800 (中国标准时间)\u0001-\u0001\u0001be8de496-02dc-d32a-71d6-31b51cca959b";


        System.out.println("wechat特殊埋点：" + getLogSourceByLogStyle.getWriteRedis(wechat));
        System.out.println("wecha无埋点：" + getLogSourceByLogStyle.getWriteRedis(freewechat));
        System.out.println("app无埋点：" + getLogSourceByLogStyle.getWriteRedis(freeapp));
    }
}
