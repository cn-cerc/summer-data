package cn.cerc.db.editor;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.FieldMeta;

public interface GetTextEvent {

    String getText(DataRow record, FieldMeta meta);

}
