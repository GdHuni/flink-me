package real_dw.entity;

/**
 * @author huni
 * @Classname HbaseTradeOrdersVo
 * @Description 存入hbase的订单实体类
 * @Date 2022/1/30 11:10
 */
public class HbaseTradeOrdersVo {

    private String dateBaseName;
    private String tableName;
    private String type;
    private String dataInfo;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDateBaseName() {
        return dateBaseName;
    }

    public void setDateBaseName(String dateBaseName) {
        this.dateBaseName = dateBaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(String dataInfo) {
        this.dataInfo = dataInfo;
    }
}
