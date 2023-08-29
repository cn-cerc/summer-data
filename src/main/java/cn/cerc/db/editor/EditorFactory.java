package cn.cerc.db.editor;

import java.util.EnumSet;

import cn.cerc.db.core.Datetime.DateType;

public class EditorFactory {

    public static OnGetSetText ofBoolean(String falseText, String trueText) {
        return new BooleanEditor(falseText, trueText);
    }

    public static OnGetSetText ofDatetime(EnumSet<DateType> options) {
        return new DatetimeEditor(options);
    }

    public static OnGetSetText ofFloat(int decimal) {
        return new FloatEditor(decimal);
    }

    public static OnGetSetText ofFloat(int decimal, String pattern) {
        return new FloatEditor(decimal, pattern);
    }

    public static OnGetSetText ofOption(String... items) {
        return new OptionEditor(items);
    }
}
