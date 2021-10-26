package cn.cerc.core;

public interface ISyncDataSet {
    void process(DataRow src, DataRow tar) throws SyncUpdateException;
}
