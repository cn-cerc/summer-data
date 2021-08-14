package cn.cerc.core;

import java.io.Serializable;

public final class FieldMeta implements Serializable {
    private static final long serialVersionUID = -6898050783447062943L;
    private String code;
    private String name;
    private String type;
    private FieldKind kind;
    private String remark;
    private boolean updateKey;
    private boolean autoincrement;

    public enum FieldKind {
        Memory, Storage, Calculated;
    }

    public FieldMeta(String code) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        this.code = code;
        this.kind = FieldKind.Memory;
    }

    public FieldMeta(String code, FieldKind kind) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        if (kind == null)
            throw new RuntimeException("fieldKind is null!");
        this.code = code;
        this.kind = kind;
    }

    public final String getCode() {
        return code;
    }

    public final String getName() {
        if (name == null)
            name = "";
        return name;
    }

    public final FieldMeta setName(String name) {
        this.name = name;
        return this;
    }

    public final String getType() {
        if (type == null)
            type = "";
        return type;
    }

    public final FieldMeta setType(String type) {
        this.type = type;
        return this;
    }

    public final FieldKind getKind() {
        return kind;
    }

    public final FieldMeta setKind(FieldKind type) {
        if (type == null)
            throw new RuntimeException("fieldKind is null!");
        if (type == FieldKind.Storage)
            throw new RuntimeException("Wrong direction of modification");
        this.kind = type;
        return this;
    }

    public final boolean isUpdateKey() {
        return updateKey;
    }

    public final String getRemark() {
        return remark;
    }

    public final void setRemark(String remark) {
        this.remark = remark;
    }

    public final FieldMeta setUpdateKey(boolean updateKey) {
        this.updateKey = updateKey;
        return this;
    }

    public final boolean isAutoincrement() {
        return autoincrement;
    }

    public final FieldMeta setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
        return this;
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
            return code.equals(meta.getCode());
        }
        return false;
    }

}
