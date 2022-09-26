package cn.cerc.db.core;

public interface HistoryLoggerImpl {

    void save(SqlQuery query, HistoryTypeEnum type, Class<? extends EntityImpl> classz);

}
