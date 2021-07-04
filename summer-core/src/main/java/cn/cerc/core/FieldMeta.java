package cn.cerc.core;

public class FieldMeta {
    private final String code;
    private final FieldType type;

    public enum FieldType {
        Data, Calculated;
    }

    public FieldMeta(String code, FieldType type) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        if (type == null)
            throw new RuntimeException("fieldType is null!");
        this.code = code;
        this.type = type;
    }

    public final String getCode() {
        return code;
    }

    public final FieldType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return code.hashCode();
    }

    @Override
    public boolean equals(Object anObject) {
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
