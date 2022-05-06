package cn.cerc.db.editor;

import java.util.EnumSet;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.FieldMeta;
import cn.cerc.db.core.Datetime.DateType;

public class DatetimeEditor implements GetSetTextEvent {
    private EnumSet<DateType> options;

    public DatetimeEditor(EnumSet<DateType> options) {
        super();
        this.options = options;
    }

    @Override
    public String getText(DataRow record, FieldMeta meta) {
        return record.getDatetime(meta.code()).setOptions(options).toString();
    }

    @Override
    public Object setText(String text) {
        return text;
    }

}
