package cn.cerc.db.editor;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.FieldMeta;

public class BooleanEditor implements GetSetTextEvent {
    private final String trueText;
    private final String falseText;

    public BooleanEditor(String falseText, String trueText) {
        super();
        this.falseText = falseText;
        this.trueText = trueText;
    }

    @Override
    public String getText(DataRow record, FieldMeta meta) {
        return record.getBoolean(meta.code()) ? trueText : falseText;
    }

    @Override
    public Boolean setText(String text) {
        return trueText.equals(text);
    }

}
