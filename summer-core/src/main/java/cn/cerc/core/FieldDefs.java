package cn.cerc.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import com.google.gson.Gson;

import cn.cerc.core.FieldMeta.FieldKind;

public final class FieldDefs implements Serializable, Iterable<FieldMeta> {
    private static final long serialVersionUID = 7478897050846245325L;
    private HashSet<FieldMeta> _items = new LinkedHashSet<>();

    public boolean exists(String fieldCode) {
        return _items.contains(new FieldMeta(fieldCode));
    }

    public boolean exists(FieldMeta field) {
        return _items.contains(field);
    }

    public List<String> names() {
        List<String> result = new ArrayList<>();
        _items.forEach(meta -> result.add(meta.getCode()));
        return result;
    }

    @Deprecated
    public List<String> getFields() {
        return names();
    }

    public FieldMeta add(String fieldCode) {
        FieldMeta item = new FieldMeta(fieldCode);
        return _items.add(item) ? item : this.getItem(fieldCode);
    }

    public FieldMeta add(String fieldCode, FieldKind fieldType) {
        FieldMeta item = new FieldMeta(fieldCode, fieldType);
        return _items.add(item) ? item : this.getItem(fieldCode);
    }

    public FieldMeta add(FieldMeta item) {
        return _items.add(item) ? item : this.getItem(item.getCode());
    }

    public void add(String... fields) {
        for (String fieldCode : fields)
            this.add(fieldCode);
    }

    public void clear() {
        _items.clear();
    }

    public int size() {
        return _items.size();
    }

    @Override
    public Iterator<FieldMeta> iterator() {
        return this._items.iterator();
    }

    public void remove(String fieldCode) {
        FieldMeta field = new FieldMeta(fieldCode);
        _items.remove(field);
    }

    @Deprecated
    public void delete(String fieldCode) {
        remove(fieldCode);
    }

    public FieldMeta get(String fieldCode) {
        for (FieldMeta meta : _items) {
            if (fieldCode.equals(meta.getCode()))
                return meta;
        }
        return null;
    }

    public FieldMeta getItems(int index) {
        int i = 0;
        for (FieldMeta meta : _items) {
            if (i == index) {
                return meta;
            }
            i++;
        }
        return null;
    }

    @Deprecated
    public FieldMeta getItem(String fieldCode) {
        return get(fieldCode);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public HashSet<FieldMeta> getItems() {
        return _items;
    }

    public void copy(FieldDefs source) {
        for (FieldMeta meta : source.getItems()) {
            this.add(meta.clone());
        }
    }

}
