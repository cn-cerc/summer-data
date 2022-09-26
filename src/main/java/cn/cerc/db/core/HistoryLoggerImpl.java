package cn.cerc.db.core;

public interface HistoryLoggerImpl {

    void save(SqlQuery query, HistoryTypeEnum type, Class<? extends EntityImpl> classz);

    default String formatHeadField(String name, String value) {
        return String.format("%s：%s", name, value);
    }

    default String formatBodyField(String name, String oldValue, String newValue) {
        return String.format("%s：%s->%s；", name, oldValue, newValue);
    }

    default String formatInsertInfo(String entityName, String headInfo) {
        return String.format("新增了 %s 主体信息为：%s；", entityName, headInfo);
    }

    default String formatDeleteInfo(String entityName, String headInfo) {
        return String.format("删除了 %s 主体信息为：%s；", entityName, headInfo);
    }

    default String formatModifyInfo(String entityName, String headInfo, String bodyInfo) {
        return String.format("%s 主体信息为：%s ；更改了：%s", entityName, headInfo, bodyInfo);
    }
}
