package cn.cerc.db.editor;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.DataRow;

public interface GetTextEvent {
    
    String getText(DataRow record, FieldMeta meta);
    
}
