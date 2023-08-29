package cn.cerc.db.editor;

import cn.cerc.db.core.DataCell;

public class BooleanEditor implements OnGetSetText {
    private final String trueText;
    private final String falseText;

    public BooleanEditor(String falseText, String trueText) {
        super();
        this.falseText = falseText;
        this.trueText = trueText;
    }

    @Override
    public String getText(DataCell data) {
        return data.getBoolean() ? trueText : falseText;
    }

    @Override
    public Boolean setText(String text) {
        return trueText.equals(text);
    }

}
