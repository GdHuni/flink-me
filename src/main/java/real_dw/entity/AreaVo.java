package real_dw.entity;

import java.io.Serializable;

/**
 * @author huni
 * @Classname AreaVo
 * @Description 地域实体类
 * @Date 2022/1/30 16:56
 */
public class AreaVo implements Serializable {
    private String id;
    private String name;
    private String pid;
    private String sname;
    private String level;
    private String citycode;
    private String yzcode;
    private String mername;
    private String Lng;
    private String Lat;
    private String pinyin;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getSname() {
        return sname;
    }

    public void setSname(String sname) {
        this.sname = sname;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getYzcode() {
        return yzcode;
    }

    public void setYzcode(String yzcode) {
        this.yzcode = yzcode;
    }

    public String getMername() {
        return mername;
    }

    public void setMername(String mername) {
        this.mername = mername;
    }

    public String getLng() {
        return Lng;
    }

    public void setLng(String lng) {
        Lng = lng;
    }

    public String getLat() {
        return Lat;
    }

    public void setLat(String lat) {
        Lat = lat;
    }

    public String getPinyin() {
        return pinyin;
    }

    public void setPinyin(String pinyin) {
        this.pinyin = pinyin;
    }

    @Override
    public String toString() {
        return "AreaVo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", pid='" + pid + '\'' +
                ", sname='" + sname + '\'' +
                ", level='" + level + '\'' +
                ", citycode='" + citycode + '\'' +
                ", yzcode='" + yzcode + '\'' +
                ", mername='" + mername + '\'' +
                ", lng='" + Lng + '\'' +
                ", lat='" + Lat + '\'' +
                ", pinyin='" + pinyin + '\'' +
                '}';
    }
}
