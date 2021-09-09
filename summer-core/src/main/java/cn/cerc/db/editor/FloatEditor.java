package cn.cerc.db.editor;

import java.text.DecimalFormat;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.Record;

public class FloatEditor implements GetSetTextEvent {
    private String pattern;
    private int decimal;

    public FloatEditor(int decimal) {
        this(decimal, "#");
    }

    public FloatEditor(int decimal, String pattern) {
        this.decimal = decimal;
        this.pattern = pattern;
    }

    @Override
    public String getText(Record record, FieldMeta meta) {
        String fmt = "0." + pattern.repeat(decimal);
        DecimalFormat df = new DecimalFormat(fmt);
        return df.format(record.getDouble(meta.getCode()));
    }

    @Override
    public Object setText(String text) {
        return Double.valueOf(text);
    }

    public final String getPattern() {
        return pattern;
    }

    public final void setPattern(String pattern) {
        this.pattern = pattern;
    }

}
