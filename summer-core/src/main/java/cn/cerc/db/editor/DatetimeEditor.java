package cn.cerc.db.editor;

import java.util.EnumSet;

import cn.cerc.core.Datetime.DateType;
import cn.cerc.core.FieldMeta;
import cn.cerc.core.DataRow;

public class DatetimeEditor implements GetSetTextEvent {
    private EnumSet<DateType> options;

    public DatetimeEditor(EnumSet<DateType> options) {
        super();
        this.options = options;
    }

    @Override
    public String getText(DataRow record, FieldMeta meta) {
        return record.getDatetime(meta.getCode()).setOptions(options).toString();
    }

    @Override
    public Object setText(String text) {
        return text;
    }

}
