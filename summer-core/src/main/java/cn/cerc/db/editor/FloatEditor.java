package cn.cerc.db.editor;

import java.math.BigDecimal;
import java.text.DecimalFormat;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.DataRow;

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
    public String getText(DataRow record, FieldMeta meta) {
        StringBuffer fmt = new StringBuffer("0.");
        for (int i = 0; i < this.decimal; i++)
            fmt.append(this.pattern);
        DecimalFormat df = new DecimalFormat(fmt.toString());
        double value = record.getDouble(meta.getCode());
        return df.format(new BigDecimal(Double.toString(value)));
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
