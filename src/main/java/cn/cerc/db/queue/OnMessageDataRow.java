package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public interface OnMessageDataRow {
    boolean execute(DataRow dataRow);
}
