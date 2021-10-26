package cn.cerc.db.core;

public interface SupportRecord {

    default DataRow asRecord() {
        DataRow result = new DataRow();
        RecordUtils.copyToRecord(this, result);
        return result;
    }

    default void copyTo(DataRow record) {
        RecordUtils.copyToRecord(this, record);
    }

}
