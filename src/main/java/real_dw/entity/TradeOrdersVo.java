package real_dw.entity;

import java.io.Serializable;

/**
 * 订单实体类
 */
public class TradeOrdersVo implements Serializable {

    private String orderId;
    private String orderNo;
    private String userId;
    private String status;
    private String productMoney;
    private String totalMoney;
    private String payMethod;
    private String isPay;
    private String areaId;
    private String tradeSrc;
    private String tradeType;
    private String isRefund;
    private String dataFlag;
    private String createTime;
    private String payTime;
    private String modifiedTime;


    @Override
    public String toString() {
        return "TradeOrdersVo{" +
                "orderId='" + orderId + '\'' +
                ", orderNo='" + orderNo + '\'' +
                ", userId='" + userId + '\'' +
                ", status='" + status + '\'' +
                ", productMoney=" + productMoney +
                ", totalMoney=" + totalMoney +
                ", payMethod='" + payMethod + '\'' +
                ", isPay='" + isPay + '\'' +
                ", areaId='" + areaId + '\'' +
                ", tradeSrc='" + tradeSrc + '\'' +
                ", tradeType='" + tradeType + '\'' +
                ", isRefund='" + isRefund + '\'' +
                ", dataFlag='" + dataFlag + '\'' +
                ", createTime='" + createTime + '\'' +
                ", payTime='" + payTime + '\'' +
                ", modifiedTime='" + modifiedTime + '\'' +
                '}';
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getProductMoney() {
        return productMoney;
    }

    public void setProductMoney(String productMoney) {
        this.productMoney = productMoney;
    }

    public String getTotalMoney() {
        return totalMoney;
    }

    public void setTotalMoney(String totalMoney) {
        this.totalMoney = totalMoney;
    }

    public String getPayMethod() {
        return payMethod;
    }

    public void setPayMethod(String payMethod) {
        this.payMethod = payMethod;
    }

    public String getIsPay() {
        return isPay;
    }

    public void setIsPay(String isPay) {
        this.isPay = isPay;
    }

    public String getAreaId() {
        return areaId;
    }

    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }

    public String getTradeSrc() {
        return tradeSrc;
    }

    public void setTradeSrc(String tradeSrc) {
        this.tradeSrc = tradeSrc;
    }

    public String getTradeType() {
        return tradeType;
    }

    public void setTradeType(String tradeType) {
        this.tradeType = tradeType;
    }

    public String getIsRefund() {
        return isRefund;
    }

    public void setIsRefund(String isRefund) {
        this.isRefund = isRefund;
    }

    public String getDataFlag() {
        return dataFlag;
    }

    public void setDataFlag(String dataFlag) {
        this.dataFlag = dataFlag;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getPayTime() {
        return payTime;
    }

    public void setPayTime(String payTime) {
        this.payTime = payTime;
    }

    public String getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(String modifiedTime) {
        this.modifiedTime = modifiedTime;
    }
}
