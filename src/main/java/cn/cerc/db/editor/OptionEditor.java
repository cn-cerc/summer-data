package cn.cerc.db.editor;

import java.util.ArrayList;
import java.util.List;

import cn.cerc.db.core.DataField;

public class OptionEditor implements OnGetSetText {
    private List<String> items = new ArrayList<>();

    public OptionEditor(String... items) {
        super();
        for (String item : items)
            this.items.add(item);
    }

    @Override
    public String getText(DataField data) {
        int index = data.getInt();
        return items.get(index);
    }

    @Override
    public Object setText(String text) {
        return items.indexOf(text);
    }
}
