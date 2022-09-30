package cn.cerc.db.editor;

import java.util.EnumSet;

import cn.cerc.db.core.DataCell;
import cn.cerc.db.core.Datetime.DateType;

public class DatetimeEditor implements OnGetSetText {
    private EnumSet<DateType> options;

    public DatetimeEditor(EnumSet<DateType> options) {
        super();
        this.options = options;
    }

    @Override
    public String getText(DataCell data) {
        return data.getDatetime().setOptions(options).toString();
    }

    @Override
    public Object setText(String text) {
        return text;
    }

}
