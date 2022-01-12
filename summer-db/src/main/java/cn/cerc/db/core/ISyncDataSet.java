package cn.cerc.db.core;

public interface ISyncDataSet {
    void process(DataRow src, DataRow tar) throws ServiceException;
}
