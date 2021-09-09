package cn.cerc.db.editor;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.Record;

public interface GetTextEvent {
    
    String getText(Record record, FieldMeta meta);
    
}
