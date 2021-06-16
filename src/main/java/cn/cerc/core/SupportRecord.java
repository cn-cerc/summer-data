package cn.cerc.core;

public interface SupportRecord {

    default Record asRecord() {
        Record result = new Record();
        RecordUtils.copyToRecord(this, result);
        return result;
    }

}
