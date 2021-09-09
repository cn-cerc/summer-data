package cn.cerc.db.editor;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.Record;

public class BooleanEditor implements GetSetTextEvent {
    private final String trueText;
    private final String falseText;

    public BooleanEditor(String falseText, String trueText) {
        super();
        this.falseText = falseText;
        this.trueText = trueText;
    }

    @Override
    public String getText(Record record, FieldMeta meta) {
        return record.getBoolean(meta.getCode()) ? trueText : falseText;
    }

    @Override
    public Boolean setText(String text) {
        return trueText.equals(text);
    }

}
