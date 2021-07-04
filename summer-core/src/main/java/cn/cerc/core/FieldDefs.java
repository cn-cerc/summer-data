package cn.cerc.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

import com.google.gson.Gson;

import cn.cerc.core.FieldMeta.FieldType;

public final class FieldDefs implements Serializable, Iterable<FieldMeta> {
    private static final long serialVersionUID = 7478897050846245325L;
    private LinkedHashSet<FieldMeta> items = new LinkedHashSet<>();

    public boolean exists(String fieldCode) {
        return items.contains(new FieldMeta(fieldCode, FieldType.Data));
    }

    public boolean exists(FieldMeta field) {
        return items.contains(field);
    }

    public List<String> getFields() {
        List<String> result = new ArrayList<>();
        items.forEach(meta -> result.add(meta.getCode()));
        return result;
    }

    public List<String> getFields(FieldType fieldType) {
        List<String> result = new ArrayList<>();
        for (FieldMeta meta : items) {
            if (fieldType == meta.getType())
                result.add(meta.getCode());
        }
        return result;
    }

    public FieldDefs add(String fieldCode) {
        items.add(new FieldMeta(fieldCode, FieldType.Data));
        return this;
    }

    public FieldDefs add(FieldMeta field) {
        items.add(field);
        return this;
    }

    public FieldMeta add(String fieldCode, FieldType fieldType) {
        FieldMeta meta = new FieldMeta(fieldCode, fieldType);
        items.add(meta);
        return meta;
    }

    public void add(String... strs) {
        for (String fieldCode : strs)
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

    @Override
    public String toString() {
        return new Gson().toJson(items);
    }

    public void delete(String fieldCode) {
        FieldMeta field = new FieldMeta(fieldCode, FieldType.Data);
        items.remove(field);
    }

    public FieldMeta getItem(String fieldCode) {
        for (FieldMeta meta : items) {
            if (fieldCode.equals(meta.getCode()))
                return meta;
        }
        return null;
    }

    public static void main(String[] args) {
        FieldDefs defs = new FieldDefs();
        defs.add("id", FieldType.Calculated);
        defs.add("id", FieldType.Data);
        defs.getItem("id").setUpdateKey(true).setAutoincrement(true);
        System.out.println(defs.size());
        defs.add("name");
        System.out.println(defs.exists("id"));
        System.out.println(defs.toString());
        defs.delete("name");
        System.out.println(defs.toString());
        defs.delete("name");
        System.out.println(defs.toString());
    }

}
