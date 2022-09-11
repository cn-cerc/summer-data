package cn.cerc.db.core;

public class DataField extends Variant {
    private DataRow dataRow;
    private String field;

    public DataField(DataRow dataRow, String field) {
        super();
        this.dataRow = dataRow;
        this.field = field;
    }

    @Override
    public Object data() {
        return dataRow.getValue(field);
    }

    @Override
    public DataField setData(Object data) {
        dataRow.setValue(field, data);
        this.setModified(true);
        return this;
    }

}
