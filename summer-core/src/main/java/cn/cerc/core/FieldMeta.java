package cn.cerc.core;

public class FieldMeta {
    private final String code;
    private FieldType type;
    private boolean updateKey;
    private boolean autoincrement;

    public enum FieldType {
        Memory, Storage, Calculated;
    }

    public FieldMeta(String code) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode not is null!");
        this.code = code;
        this.type = FieldType.Memory;
    }

    public FieldMeta(String code, FieldType type) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode not is null!");
        if (type == null)
            throw new RuntimeException("fieldType not is null!");
        this.code = code;
        this.type = type;
    }

    public final String getCode() {
        return code;
    }

    public final FieldType getType() {
        return type;
    }

    public final FieldMeta setType(FieldType type) {
        if (type == null)
            throw new RuntimeException("fieldType not is null!");
        if (type == FieldType.Storage)
            throw new RuntimeException("Wrong direction of modification");
        this.type = type;
        return this;
    }

    public final boolean isUpdateKey() {
        return updateKey;
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
