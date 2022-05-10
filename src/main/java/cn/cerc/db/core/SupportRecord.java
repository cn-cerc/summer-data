package cn.cerc.db.core;

@Deprecated
public interface SupportRecord extends EntityImpl {

    @Deprecated
    default DataRow asRecord() {
        return new DataRow().loadFromEntity(this);
    }

    @Deprecated
    default void copyTo(DataRow record) {
        record.loadFromEntity(this);
    }

}
