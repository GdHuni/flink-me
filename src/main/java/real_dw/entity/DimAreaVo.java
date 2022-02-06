package real_dw.entity;

import java.io.Serializable;

/**
 * @author huni
 * @Classname AreaVo
 * @Description dim层地域实体类
 * @Date 2022/1/30 16:56
 */
public class DimAreaVo implements Serializable {

    /**区的id */
    private int areaId;
    /**区的名字 */
    private String aname;
    /**城市的id */
    private int cid;
    /**城市的名字 */
    private String city;
    /**省份的id */
    private int proId;
    /**省份的名字 */
    private String province;

    public DimAreaVo() {
    }

    public DimAreaVo(int areaId, String aname, int cid, String city, int proId, String province) {
        this.areaId = areaId;
        this.aname = aname;
        this.cid = cid;
        this.city = city;
        this.proId = proId;
        this.province = province;
    }

    public int getAreaId() {
        return areaId;
    }

    public void setAreaId(int areaId) {
        this.areaId = areaId;
    }

    public String getAname() {
        return aname;
    }

    public void setAname(String aname) {
        this.aname = aname;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getProId() {
        return proId;
    }

    public void setProId(int proId) {
        this.proId = proId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "DimAreaVo{" +
                "areaId=" + areaId +
                ", aname='" + aname + '\'' +
                ", cid=" + cid +
                ", city='" + city + '\'' +
                ", proId=" + proId +
                ", province='" + province + '\'' +
                '}';
    }
}
