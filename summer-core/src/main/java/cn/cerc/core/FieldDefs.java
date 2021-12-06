package cn.cerc.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.google.gson.Gson;

import cn.cerc.core.FieldMeta.FieldKind;

public final class FieldDefs implements Serializable, Iterable<FieldMeta> {
    private static final long serialVersionUID = 7478897050846245325L;
    private HashSet<FieldMeta> items = new LinkedHashSet<>();

    public boolean exists(String fieldCode) {
        return items.contains(new FieldMeta(fieldCode));
    }

    public boolean exists(FieldMeta field) {
        return items.contains(field);
    }

    public List<String> names() {
        List<String> result = new ArrayList<>();
        items.forEach(meta -> result.add(meta.getCode()));
        return result;
    }

    @Deprecated
    public List<String> getFields() {
        return names();
    }

    public FieldMeta add(String fieldCode) {
        FieldMeta item = new FieldMeta(fieldCode);
        return items.add(item) ? item : this.getItem(fieldCode);
    }

    public FieldMeta add(String fieldCode, FieldKind fieldType) {
        FieldMeta item = new FieldMeta(fieldCode, fieldType);
        return items.add(item) ? item : this.getItem(fieldCode);
    }

    public FieldMeta add(FieldMeta item) {
        return items.add(item) ? item : this.getItem(item.getCode());
    }

    @Deprecated
    public void add(String... fields) {
        for (String fieldCode : fields)
            this.add(fieldCode);
    }

    public void clear() {
        items.clear();
    }

    public int size() {
        return items.size();
    }

    @Override
    public Iterator<FieldMeta> iterator() {
        return this.items.iterator();
    }

    public void remove(String fieldCode) {
        FieldMeta field = new FieldMeta(fieldCode);
        items.remove(field);
    }

    @Deprecated
    public void delete(String fieldCode) {
        remove(fieldCode);
    }

    public FieldMeta get(String fieldCode) {
        for (FieldMeta meta : items) {
            if (fieldCode.equals(meta.getCode()))
                return meta;
        }
        return null;
    }

    public FieldMeta getItems(int index) {
        int i = 0;
        for (FieldMeta meta : items) {
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
        return items;
    }

    public void copy(FieldDefs source) {
        for (FieldMeta meta : source.getItems()) {
            this.add(meta.clone());
        }
    }

    public FieldDefs readDefine(Class<?> clazz, String... names) {
        List<String> items = null;
        if (names.length > 0)
            items = Arrays.asList(names);
        for (Field field : clazz.getDeclaredFields()) {
            if (items != null && items.indexOf(field.getName()) == -1)
                continue;
            FieldMeta meta = this.get(field.getName());
            if (meta != null) {
                Column column = field.getDeclaredAnnotation(Column.class);
                if (column != null) {
                    meta.setName(column.name());
                    if (field.getType().isEnum()) {
                        Enumerated enumerated = field.getDeclaredAnnotation(Enumerated.class);
                        if ((enumerated != null) && (enumerated.value() == EnumType.STRING))
                            meta.setType("s" + column.length());
                        else
                            meta.setType("n1");
                    } else {
                        meta.getFieldType().setType(field.getType());
                        if ("s".equals(meta.getType()) || "o".equals(meta.getType()))
                            meta.getFieldType().setLength(column.length());
                    }
                }
            }
        }
        return this;
    }

}
