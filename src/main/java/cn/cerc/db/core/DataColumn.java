package cn.cerc.db.core;

public class DataColumn extends Variant {
    private DataSet dataSet;

    public DataColumn(DataSet dataSet, String field) {
        super();
        this.dataSet = dataSet;
        this.setKey(field);
    }

    public DataSet source() {
        return dataSet;
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
        return dataSet.current();
    }

}
