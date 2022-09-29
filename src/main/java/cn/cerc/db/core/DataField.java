package cn.cerc.db.core;

public class DataField extends Variant {
    private DataRow source;

    public DataField(DataRow source, String field) {
        super();
        this.source = source;
        this.setKey(field);
    }

    public DataRow source() {
        return this.source;
    }

    @Override
    public Object value() {
        return source.getValue(key());
    }

    @Override
    public DataField setValue(Object value) {
        source.setValue(key(), value);
        setModified(true);
        return this;
    }

    @Override
    public boolean hasValue() {
        return source.has(key());
    }

}
