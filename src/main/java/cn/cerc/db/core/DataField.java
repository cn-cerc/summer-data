package cn.cerc.db.core;

public class DataField extends Variant {
    private DataRow dataRow;

    public DataField(DataRow dataRow, String field) {
        super();
        this.dataRow = dataRow;
        this.setKey(field);
    }

    public DataRow dataRow() {
        return this.dataRow;
    }

    @Override
    public Object value() {
        return dataRow.getValue(key());
    }

    @Override
    public DataField setValue(Object value) {
        dataRow.setValue(key(), value);
        setModified(true);
        return this;
    }

    @Override
    public boolean hasValue() {
        return dataRow.has(key());
    }

}
