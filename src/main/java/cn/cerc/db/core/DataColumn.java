package cn.cerc.db.core;

public class DataColumn extends Variant {
    private DataSetSource source;

    public DataColumn(DataSetSource source, String field) {
        super();
        this.source = source;
        this.setKey(field);
    }

    public DataSetSource source() {
        return source;
    }

    @Override
    public Object value() {
        return current().getValue(key());
    }

    @Override
    public DataColumn setValue(Object value) {
        current().setValue(key(), value);
        setModified(true);
        return this;
    }

    @Override
    public boolean hasValue() {
        return current().hasValue(key());
    }

    private DataRow current() {
        return source.getDataSet().map(ds -> ds.current()).orElse(null);
    }

}
