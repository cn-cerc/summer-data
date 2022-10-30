package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public interface OnReceiveDataRow {
    boolean execute(DataRow dataRow);
}
