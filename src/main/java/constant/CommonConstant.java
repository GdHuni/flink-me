package constant;

import java.util.HashMap;

/**
 * 公共常量类
 * @相对路径: com.lyj.constant.CommonConstant
 * @author  <a href="mailto:zhouh@leyoujia.com">周虎</a>
 * @创建日期 2021/12/07 18:38
 */
public class CommonConstant {

    public static String report_mapping = "analysis/report/toDetail?id";
    public static String funnel_mapping = "analysis/funnel/detail?id";
    public static String newreport_mapping = "analysis/newreport/detail?id";
    public static String retained_mapping = "analysis/retained/detail?id";
    public static String analysis_pattern = ".*/analysis/.*id=\\d.*";


    /**
     * 通用数字常量
     */
    public static Integer INT_0 = 0;
    public static String STR_0 = "0";
    public static Integer INT_1 = 1;
    public static String STR_1 = "1";
    public static Integer INT_2 = 2;
    public static String STR_2 = "2";
    public static Integer INT_3 = 3;
    public static String STR_3 = "3";
    public static Integer INT_4 = 4;
    public static String STR_4 = "4";
    public static Integer INT_5 = 5;
    public static String STR_5 = "5";
    public static Integer INT_6 = 6;
    public static String STR_6 = "6";
    public static Integer INT_7 = 7;
    public static String STR_7 = "7";
    public static Integer INT_8 = 8;
    public static String STR_8 = "8";
    public static Integer INT_9 = 9;
    public static String STR_9 = "9";
    public static Integer INT_10 = 10;
    public static String STR_10 = "10";
    public static Integer INT_14 = 14;
    public static String STR_14 = "14";
    public static Integer INT_30 = 30;
    public static String STR_30 = "30";
    public static Integer INT_60 = 60;
    public static String STR_60 = "60";
    public static Integer INT_100 = 100;
    public static String STR_100 = "100";
    public static Integer INT_200 = 200;
    public static String STR_200 = "200";
    public static Integer INT_1000 = 1000;
    public static String STR_1000 = "1000";
    public static Integer INT_5000 = 5000;
    public static String STR_5000 = "5000";
    public static final int ERROR_MESSAGE_LEN = 4999;
    public static String STATUS_CODE = "code";


    /** 通配符 */
    public static String WILDCARD = "*";
    public static String PERCENT = "%";
    public static String WILDCARD_JH = "#";
    public static String SIGN_COMMA = ",";
    public static String SIGN_HG = "-";
    public static String DIMS = "dims";
    public static String FIELD = "field";
    public static String COMPLEX_INDEX = "complexIndex";
    /**
     * 斜杆
     */
    public static String DIAGONAL_BAR = "/";
    /**
     * 下划线
     */
    public static String HORIZONTAL_BAR = "_";

    public static String FALSE_STR = "false";


    /**
     * 大数据部门number
     */
    public static final String BIGDATA_DEPT_NUMBER = "7701300";

    /** 指标保存为源表的前缀 */
    public static final String PREFIX_METRICS = "view_metrics_";

    /**视图前缀*/
    public static final String VIEW_PREFIX="view_";

    /** ALL */
    public static final String ALL = "'all'";
    public static final String ALL_1 = "all";
    /** view_ */
    public static final String VIEW_ = "view_";
    /** -9999999 */
    public static final String STR_9999999 = "'-9999999'";
    /** 部门 */
    public static final String DEPT_NAME_TXT = "部门";
    /** 部门 */
    public static final String PURVIEW_SUFFIX = "_purview";
    /** 字符串 */
    public static final String STRING_EN = "string";
    /** 字符串 */
    public static final String STRING_CN = "字符串";
    /** 1,2 */
    public static final String STR_1_2 = "1,2";
    /** 1,3 */
    public static final String STR_1_3 = "1,3";
    /** 1,3 */
    public static final String STR_1_2_3 = "1,2,3";
    /** 2,3 */
    public static final String STR_2_3 = "2,3";

    /** 转化周期 */
    public final static HashMap<Integer,String> CONVERT_GRANULARITY_MAP = new HashMap<Integer,String>(4){
        {
            put(1,"天");
            put(2,"周");
            put(3,"月");
        }
    };
    /** 展现形式 1 留存 2变化 */
    public final static HashMap<Integer,String> SHOW_TYPE_MAP = new HashMap<Integer,String>(4){
        {
            put(1,"留存");
            put(2,"变化");
        }
    };
}
