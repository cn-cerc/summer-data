package cn.cerc.db.log;

import java.util.ArrayList;

public class KnowallData extends Throwable {
    private static final long serialVersionUID = -6758028712431145650L;

    private ArrayList<String> data = new ArrayList<>();

    public KnowallData addData(String data) {
        if (this.data.size() < 10)
            this.data.add(data);
        return this;
    }

    public String getData(int index) {
        return data.get(index);
    }

    public int getDataCount() {
        return data == null ? 0 : data.size();
    }

}
