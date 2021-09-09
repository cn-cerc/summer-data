package cn.cerc.db.editor;

import java.util.ArrayList;
import java.util.List;

import cn.cerc.core.FieldMeta;
import cn.cerc.core.Record;

public class OptionEditor implements GetSetTextEvent {
    private List<String> items = new ArrayList<>();

    public OptionEditor(String... items) {
        super();
        for (String item : items)
            this.items.add(item);
    }

    @Override
    public String getText(Record record, FieldMeta meta) {
        int index = record.getInt(meta.getCode());
        return items.get(index);
    }

    @Override
    public Object setText(String text) {
        return items.indexOf(text);
    }
}
