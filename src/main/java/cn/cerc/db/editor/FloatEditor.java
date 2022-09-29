package cn.cerc.db.editor;

import java.math.BigDecimal;
import java.text.DecimalFormat;

import cn.cerc.db.core.DataField;

public class FloatEditor implements OnGetSetText {
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
    public String getText(DataField data) {
        StringBuffer fmt = new StringBuffer("0.");
        for (int i = 0; i < this.decimal; i++)
            fmt.append(this.pattern);
        DecimalFormat df = new DecimalFormat(fmt.toString());
        double value = data.getDouble();
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
