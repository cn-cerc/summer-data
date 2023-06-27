package cn.cerc.db.queue.entity;

import cn.cerc.db.core.Datetime;

public class CheckMQEntity {
    private String projcet;
    private String version;
    private Datetime sendTime;
    private boolean isAlive;

    public Datetime getSendTime() {
        return sendTime;
    }

    public void setSendTime(Datetime sendTime) {
        this.sendTime = sendTime;
    }

    public String getProjcet() {
        return projcet;
    }

    public void setProjcet(String projcet) {
        this.projcet = projcet;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAlive(boolean isAlive) {
        this.isAlive = isAlive;
    }
}
