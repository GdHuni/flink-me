package real_dw.entity;



/**
 * @author huni
 * @Classname HbaseTradeOrdersVo
 * @Description 存入hbase的订单实体类
 * @Date 2022/1/30 11:10
 */
public class DimTradeOrder {
    private int orderId;
    private String orderNo;
    private int userId;
    private int status;
    private Double totalMoney;
    private int areaId;

    public DimTradeOrder() {
    }

    public DimTradeOrder(int orderId, String orderNo, int userId, int status, Double totalMoney, int areaId) {
        this.orderId = orderId;
        this.orderNo = orderNo;
        this.userId = userId;
        this.status = status;
        this.totalMoney = totalMoney;
        this.areaId = areaId;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Double getTotalMoney() {
        return totalMoney;
    }

    public void setTotalMoney(Double totalMoney) {
        this.totalMoney = totalMoney;
    }

    public int getAreaId() {
        return areaId;
    }

    public void setAreaId(int areaId) {
        this.areaId = areaId;
    }

    @Override
    public String toString() {
        return "DimTradeOrder{" +
                "orderId=" + orderId +
                ", orderNo='" + orderNo + '\'' +
                ", userId=" + userId +
                ", status=" + status +
                ", totalMoney=" + totalMoney +
                ", areaId=" + areaId +
                '}';
    }
}

