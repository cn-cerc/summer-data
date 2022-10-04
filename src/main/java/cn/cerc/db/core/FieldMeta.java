package cn.cerc.db.core;

import java.io.Serializable;
import java.lang.reflect.Field;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Version;

import com.google.gson.Gson;

import cn.cerc.db.editor.OnGetSetText;
import cn.cerc.db.editor.OnGetText;
import cn.cerc.db.editor.OnSetText;

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
    // 字段是否标识history
    private History history = null;
    // 建议显示宽度
    private int width = 0;
    // 在增加记录时，是否为必填栏位
    private boolean required = false;

    // UI取值事件
    private OnGetText onGetText;
    private OnSetText onSetText;

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
        result.onGetText = this.onGetText;
        result.onSetText = this.onSetText;
        result.width = this.width;
        return result;
    }

    public final String code() {
        return code;
    }

//    @Deprecated
//    public final String getCode() {
//        return code();
//    }

    public final String name() {
        return name;
    }

//    @Deprecated
//    public final String getName() {
//        return name();
//    }

    public final FieldMeta setName(String name) {
        this.name = name;
        return this;
    }

    public final String typeValue() {
        return dataType().value();
    }

//    @Deprecated
//    public final void setType(Class<?> clazz) {
//        dataType().readClass(clazz);
//    }

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
            throw new RuntimeException("value is null!");
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

//    @Deprecated
//    public final boolean isUpdateKey() {
//        return identification();
//    }

    public final String remark() {
        return remark;
    }

    public final FieldMeta setRemark(String remark) {
        this.remark = remark;
        return this;
    }

//    @Deprecated
//    public final FieldMeta setUpdateKey(boolean uid) {
//        return this.setIdentification(uid);
//    }

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

    public FieldMeta onGetText(OnGetText onGetText) {
        this.onGetText = onGetText;
        return this;
    }

    public OnGetText onGetText() {
        return this.onGetText;
    }

    public FieldMeta onSetText(OnSetText onSetText) {
        this.onSetText = onSetText;
        return this;
    }

    public OnSetText onSetText() {
        return this.onSetText;
    }

    public String getText(DataRow row) {
        if (onGetText == null)
            return row.getString(code);
        return onGetText.getText(new DataCell(row, code));
    }

    public Object setText(String value) {
        if (onSetText == null)
            return value;
        return onSetText.setText(value);
    }

    public void onGetSetText(OnGetSetText getsetTextEvent) {
        this.onGetText(getsetTextEvent);
        this.onSetText(getsetTextEvent);
    }

    public String json() {
        return new Gson().toJson(this);
    }

    public History history() {
        return history;
    }

    public FieldMeta setHistory(History history) {
        this.history = history;
        return this;
    }

    public int width() {
        return this.width;
    }

    public FieldMeta setWidth(int width) {
        this.width = width;
        return this;
    }

    public boolean required() {
        return this.required;
    }

    public FieldMeta setRequired(boolean required) {
        this.required = required;
        return this;
    }

    /**
     * 从Entity类读取属到到当前FieldMeta
     * 
     * @param entityClass Entity 对象
     * @return 若读取成功，返回true
     */
    public boolean readEntity(Class<?> entityClass) {
        boolean result = false;
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals(this.code)) {
                readEntityField(field);
                result = true;
                break;
            }
        }
        return result;
    }

    public void readEntityField(Field field) {
        Describe describe = field.getDeclaredAnnotation(Describe.class);
        if (describe != null) {
            if (!"".equals(describe.name()))
                this.setName(describe.name());
            if (!"".equals(describe.remark()))
                this.setRemark(describe.remark());
            this.setWidth(describe.width());
            this.setRequired(describe.required());
        }
        Column column = field.getDeclaredAnnotation(Column.class);
        if (column != null) {
            this.setInsertable(column.insertable());
            this.setUpdatable(column.updatable());
            this.setNullable(column.nullable());
        }

        this.setHistory(field.getDeclaredAnnotation(History.class));

        Id id = field.getDeclaredAnnotation(Id.class);
        if (id != null) {
            this.setIdentification(true);
            this.setNullable(false);
        }
        GeneratedValue gv = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gv != null) {
            if (gv.strategy() != GenerationType.AUTO)
                throw new RuntimeException("strategy only support auto");
            this.setAutoincrement(true);
            this.setInsertable(false);
            this.setUpdatable(false);
        }
        if (field.getType().isEnum()) {
            Enumerated enumerated = field.getDeclaredAnnotation(Enumerated.class);
            if ((enumerated != null) && (enumerated.value() == EnumType.STRING))
                this.dataType().setValue("s" + column.length());
            else
                this.dataType().setValue("n1");
        } else {
            this.dataType().setClass(field.getType());
            if ("s".equals(this.dataType().value()) || "o".equals(this.dataType().value()))
                this.dataType().setLength(column.length());
        }
        Version version = field.getDeclaredAnnotation(Version.class);
        if (version != null)
            this.setNullable(false);
    }

}
