package cn.cerc.db.log;

import java.util.ArrayList;

import cn.cerc.db.core.Datetime;

public class KnowallLog extends Throwable {
    private static final long serialVersionUID = -6758028712431145650L;
    private Datetime createTime;
    private String origin;
    private String level;
    private String message;
    private String group;
    private ArrayList<String> datas;
    private String machine;

    public KnowallLog(String origin) {
        this.createTime = new Datetime();
        this.origin = origin;
        this.level = "info";
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public ArrayList<String> getParams() {
        return datas;
    }

    public void setParams(ArrayList<String> params) {
        this.datas = params;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getMachine() {
        return machine;
    }

    public void setMachine(String machine) {
        this.machine = machine;
    }

    public Datetime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Datetime createTime) {
        this.createTime = createTime;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean addData(String data) {
        if (datas == null)
            this.datas = new ArrayList<>();
        if (datas.size() < 10)
            return this.datas.add(data);
        else
            return false;
    }

    public String getData(int index) {
        return datas.get(index);
    }

    public int getDataCount() {
        return datas == null ? 0 : datas.size();
    }

    public boolean post() {
        // FIXME 提交到 knowall.cn
        return false;
    }
}
