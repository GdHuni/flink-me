package real_dw.entity;

import java.util.Objects;

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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HbaseTradeOrdersVo that = (HbaseTradeOrdersVo) o;
        return Objects.equals(dateBaseName, that.dateBaseName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(type, that.type) &&
                Objects.equals(dataInfo, that.dataInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateBaseName, tableName, type, dataInfo);
    }

    @Override
    public String toString() {
        return "HbaseTradeOrdersVo{" +
                "dateBaseName='" + dateBaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", type='" + type + '\'' +
                ", dataInfo='" + dataInfo + '\'' +
                '}';
    }
}
