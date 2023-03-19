package cn.cerc.db.core;

public class DataColumn extends Variant {
    private DataSource source;

    public DataColumn(DataSource source, String field) {
        super();
        this.source = source;
        this.setKey(field);
    }

    public DataSource source() {
        return source;
    }

    @Override
    public Object value() {
        return source.current().getValue(key());
    }

    @Override
    public DataColumn setValue(Object value) {
        source.current().setValue(key(), value);
        setModified(true);
        return this;
    }

    @Override
    public boolean hasValue() {
        return source.current().hasValue(key());
    }

}
