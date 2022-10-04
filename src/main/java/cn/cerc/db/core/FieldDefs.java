package cn.cerc.db.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;

import com.google.gson.Gson;

import cn.cerc.db.core.FieldMeta.FieldKind;

public final class FieldDefs implements Serializable, Iterable<FieldMeta> {
    private static final long serialVersionUID = 7478897050846245325L;
    private HashSet<FieldMeta> items = new LinkedHashSet<>();

    public FieldDefs() {
        super();
    }

    /**
     * 读入实体字段
     * 
     * @param clazz entity class
     */
    public FieldDefs(Class<?> clazz) {
        super();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.getDeclaredAnnotation(Id.class) != null || field.getDeclaredAnnotation(Column.class) != null) {
                String fieldCode = field.getName();
                FieldMeta meta = this.add(fieldCode, FieldKind.Storage);
                meta.readEntityField(field);
            }
        }
    }

    public boolean exists(String fieldCode) {
        return items.contains(new FieldMeta(fieldCode));
    }

    public boolean exists(FieldMeta field) {
        return items.contains(field);
    }

    public List<String> names() {
        List<String> result = new ArrayList<>();
        items.forEach(meta -> result.add(meta.code()));
        return result;
    }

    @Deprecated
    public List<String> getFields() {
        return names();
    }

    public FieldMeta add(String fieldCode) {
        FieldMeta item = new FieldMeta(fieldCode);
        return items.add(item) ? item : this.get(fieldCode);
    }

    public FieldMeta add(String fieldCode, FieldKind fieldType) {
        FieldMeta item = new FieldMeta(fieldCode, fieldType);
        return items.add(item) ? item : this.get(fieldCode);
    }

    public FieldMeta add(FieldMeta item) {
        return items.add(item) ? item : this.get(item.code());
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
            if (fieldCode.equals(meta.code()))
                return meta;
        }
        return null;
    }

    public FieldMeta get(int index) {
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
    public final FieldMeta getItems(int index) {
        return get(index);
    }

    @Deprecated
    public FieldMeta getItem(String fieldCode) {
        return get(fieldCode);
    }

    @Override
    public String toString() {
        return json();
    }

    public void copyFrom(FieldDefs source) {
        for (FieldMeta meta : source.getItems())
            this.add(meta.clone());
    }

    public void copyTo(FieldDefs target) {
        target.copyFrom(this);
    }

    public FieldDefs readDefine(Class<?> clazz, String... names) {
        List<String> items = null;
        if (names.length > 0)
            items = Arrays.asList(names);
        for (Field field : clazz.getDeclaredFields()) {
            if (items != null && items.indexOf(field.getName()) == -1)
                continue;
            FieldMeta meta = this.get(field.getName());
            if (meta != null)
                meta.readEntityField(field);
        }
        return this;
    }

    public final FieldMeta getByAutoincrement() {
        FieldMeta result = null;
        boolean find = false;
        for (FieldMeta meta : this.items) {
            if (meta.storage() && meta.autoincrement()) {
                if (find)
                    throw new RuntimeException("only support one Autoincrement field");
                find = true;
                result = meta;
            }
        }
        return result;
    }

    public HashSet<FieldMeta> getItems() {
        return items;
    }

    public String json() {
        return new Gson().toJson(items);
    }

}
