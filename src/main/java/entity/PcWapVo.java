package entity;

import java.util.Objects;

/**
 * @Classname PcWapVo
 * @Description TODO
 * @Date 2021/12/3 11:20
 * @Created by huni
 */
public class PcWapVo {
    private String loc;
    private String wid;
    private String type;
    private String createWorkerId;
    private String reportId;
    private String l_date;
    private long pv;
    private long uv;
    private long ts;
    public PcWapVo(String loc, String type, String reportId, String l_date, String wid,long ts) {
        this.loc = loc;
        this.type = type;
        this.reportId = reportId;
        this.l_date = l_date;
        this.wid = wid;
        this.ts = ts;
    }

    public PcWapVo(String loc, String wid, String type, String createWorkerId, String reportId, long pv, long uv) {
        this.loc = loc;
        this.wid = wid;
        this.type = type;
        this.createWorkerId = createWorkerId;
        this.reportId = reportId;
        this.pv = pv;
        this.uv = uv;
    }

    public PcWapVo(String loc, String wid, String type) {
        this.loc = loc;
        this.wid = wid;
        this.type = type;
    }

    public PcWapVo() {
    }

    public String getL_date() {
        return l_date;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public void setL_date(String l_date) {
        this.l_date = l_date;
    }

    public String getCreateWorkerId() {
        return createWorkerId;
    }

    public void setCreateWorkerId(String createWorkerId) {
        this.createWorkerId = createWorkerId;
    }

    public String getReportId() {
        return reportId;
    }

    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public long getUv() {
        return uv;
    }

    public void setUv(long uv) {
        this.uv = uv;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PcWapVo pcWapVo = (PcWapVo) o;
        return Objects.equals(loc, pcWapVo.loc) && Objects.equals(wid, pcWapVo.wid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loc, wid);
    }

    @Override
    public String toString() {
        return "PcWapVo{" +
                "loc='" + loc + '\'' +
                ", wid='" + wid + '\'' +
                ", type='" + type + '\'' +
                ", createWorkerId='" + createWorkerId + '\'' +
                ", reportId='" + reportId + '\'' +
                ", l_date='" + l_date + '\'' +
                ", pv=" + pv +
                ", uv=" + uv +
                '}';
    }
}
