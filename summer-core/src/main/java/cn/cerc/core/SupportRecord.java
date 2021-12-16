package cn.cerc.core;

@Deprecated
public interface SupportRecord {

    @Deprecated
    default DataRow asRecord() {
        return new DataRow().loadFromEntity(this);
    }

    @Deprecated
    default void copyTo(DataRow record) {
        record.loadFromEntity(this);
    }

}
