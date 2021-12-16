package cn.cerc.core;

import java.io.Serializable;

import com.google.gson.Gson;

import cn.cerc.db.editor.GetSetTextEvent;
import cn.cerc.db.editor.GetTextEvent;
import cn.cerc.db.editor.SetTextEvent;

public final class FieldMeta implements Serializable {
    private static final long serialVersionUID = -6898050783447062943L;
    private String code;
    private String name;
    private DataType dataType;
    private String remark;
    private FieldKind kind = FieldKind.Memory;
    // 唯一标识
    private boolean identification = false;
    // 是否为自增字段
    private boolean autoincrement = false;
    // 是否参与持久化插入
    private boolean insertable = true;
    // 是否参与持久化更新
    private boolean updatable = true;
    // 是否允许为空
    private boolean nullable = true;
    // UI取值事件
    private GetTextEvent onGetTextEvent;
    private SetTextEvent onSetTextEvent;

    public enum FieldKind {
        Memory, Storage, Calculated;
    }

    public FieldMeta(String code) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        this.code = code;
    }

    public FieldMeta(String code, FieldKind kind) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        if (kind == null)
            throw new RuntimeException("fieldKind is null!");
        this.code = code;
        this.setKind(kind);
    }

    @Override
    public FieldMeta clone() {
        FieldMeta result = new FieldMeta(this.code);
        result.name = this.name;
        if (this.dataType != null)
            result.dataType = this.dataType.clone();
        result.kind = this.kind;
        result.remark = this.remark;
        result.identification = this.identification;
        result.autoincrement = this.autoincrement;
        result.insertable = this.insertable;
        result.updatable = this.updatable;
        result.onGetTextEvent = this.onGetTextEvent;
        result.onSetTextEvent = this.onSetTextEvent;
        return result;
    }

    public final String code() {
        return code;
    }

    @Deprecated
    public final String getCode() {
        return code();
    }

    public final String name() {
        return name;
    }

    @Deprecated
    public final String getName() {
        return name();
    }

    public final FieldMeta setName(String name) {
        this.name = name;
        return this;
    }

    public final String typeValue() {
        return dataType().value();
    }

    @Deprecated
    public final void setType(Class<?> clazz) {
        dataType().readClass(clazz);
    }

    public final DataType dataType() {
        if (this.dataType == null)
            this.dataType = new DataType();
        return dataType;
    }

    public final FieldKind kind() {
        return kind;
    }

    public final FieldMeta setKind(FieldKind value) {
        if (value == null)
            throw new RuntimeException("fieldKind is null!");
        if (kind != value) {
            this.kind = value;
            if (value == FieldKind.Storage) {
                this.setUpdatable(true);
                this.setInsertable(true);
            } else {
                this.setUpdatable(false);
                this.setInsertable(false);
            }
        }
        return this;
    }

    @Deprecated
    public final boolean isUpdateKey() {
        return identification();
    }

    public final String remark() {
        return remark;
    }

    public final FieldMeta setRemark(String remark) {
        this.remark = remark;
        return this;
    }

    @Deprecated
    public final FieldMeta setUpdateKey(boolean uid) {
        return this.setIdentification(uid);
    }

    public final boolean identification() {
        return identification;
    }

    public final FieldMeta setIdentification(boolean value) {
        if (this.identification != value)
            this.identification = value;
        return this;
    }

    public final boolean autoincrement() {
        return autoincrement;
    }

    public final FieldMeta setAutoincrement(boolean value) {
        if (this.autoincrement != value)
            this.autoincrement = value;
        return this;
    }

    public boolean insertable() {
        if ((kind == FieldKind.Storage))
            return this.insertable;
        else if (this.insertable)
            throw new RuntimeException("kind not is storage");
        return false;
    }

    public void setInsertable(boolean value) {
        if (this.updatable != value) {
            if (value && kind != FieldKind.Storage)
                throw new RuntimeException("kind not is storage");
            this.insertable = value;
        }
    }

    public boolean updatable() {
        if (kind == FieldKind.Storage)
            return this.updatable;
        else if (this.updatable)
            throw new RuntimeException("updatable is true");
        return false;
    }

    public FieldMeta setUpdatable(boolean value) {
        if (this.updatable != value) {
            if (value && kind != FieldKind.Storage)
                throw new RuntimeException("kind not is storage");
            this.updatable = value;
        }
        return this;
    }

    public boolean storage() {
        return kind == FieldKind.Storage;
    }

    public boolean calculated() {
        return kind == FieldKind.Calculated;
    }

    public boolean nullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public final int hashCode() {
        return code.hashCode();
    }

    @Override
    public final boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof FieldMeta) {
            FieldMeta meta = (FieldMeta) anObject;
            return code.equals(meta.code());
        }
        return false;
    }

    public FieldMeta onGetText(GetTextEvent getTextEvent) {
        this.onGetTextEvent = getTextEvent;
        return this;
    }

    public FieldMeta onSetText(SetTextEvent setTextEvent) {
        this.onSetTextEvent = setTextEvent;
        return this;
    }

    public String getText(DataRow record) {
        if (onGetTextEvent == null)
            return record.getString(code);
        return onGetTextEvent.getText(record, this);
    }

    public Object setText(String value) {
        if (onSetTextEvent == null)
            return value;
        return onSetTextEvent.setText(value);
    }

    public void onGetSetText(GetSetTextEvent getsetTextEvent) {
        this.onGetText(getsetTextEvent);
        this.onSetText(getsetTextEvent);
    }

    public String json() {
        return new Gson().toJson(this);
    }

}
