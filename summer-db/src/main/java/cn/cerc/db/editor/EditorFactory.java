package cn.cerc.db.editor;

import java.util.EnumSet;

import cn.cerc.core.Datetime.DateType;

public class EditorFactory {

    public static GetSetTextEvent ofBoolean(String falseText, String trueText) {
        return new BooleanEditor(falseText, trueText);
    }

    public static GetSetTextEvent ofDatetime(EnumSet<DateType> options) {
        return new DatetimeEditor(options);
    }

    public static GetSetTextEvent ofFloat(int decimal) {
        return new FloatEditor(decimal);
    }

    public static GetSetTextEvent ofFloat(int decimal, String pattern) {
        return new FloatEditor(decimal, pattern);
    }

    public static GetSetTextEvent ofOption(String... items) {
        return new OptionEditor(items);
    }
}
