package cn.cerc.db.core;

public class DataColumn extends Variant {
    private DataSource dataSource;

    public DataColumn(DataSource dataSource, String field) {
        super();
        this.dataSource = dataSource;
        this.setKey(field);
    }

    public DataSource dataSource() {
        return dataSource;
    }

    @Override
    public Object value() {
        return dataSource.current().getValue(key());
    }

    @Override
    public DataColumn setValue(Object value) {
        dataSource.current().setValue(key(), value);
        setModified(true);
        return this;
    }

    @Override
    public boolean hasValue() {
        return dataSource.current().has(key());
    }

}
