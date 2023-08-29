package cn.cerc.db.core;

import java.util.Map;
import java.util.Objects;

public interface EntityImpl {

    /**
     * @return EntityQuery
     */
    EntityHomeImpl getEntityHome();

    /**
     * 设置EntityQuery
     * 
     * @param entityHome EntityQuery
     */
    void setEntityHome(EntityHomeImpl entityHome);

    /**
     * 插入新记录时，判断字段是否允许为空，若不允许为空，则设置默值
     * 
     * @param handle IHandle
     */
    default void onInsertPost(IHandle handle) {

    }

    /**
     * 更新记录时自动更新时间戳
     * 
     * @param handle IHandle
     */
    default void onUpdatePost(IHandle handle) {

    }

    /**
     * 更新记录时自动更新时间戳
     * 
     * @param handle IHandle
     */
    default HistoryLoggerImpl getHistoryLogger() {
        return null;
    }

    /**
     * 输出时自动带出计算字段
     * 
     * @param DataRow    关连的数据源
     * @param manyEntity 关连的对象
     */
    default void onJoinName(DataRow sender, Class<? extends EntityImpl> clazz, Map<String, String> items) {

    }

    /**
     * 用途：若post前，不能确认entity在query中的位置，可以先使用此功能进行确认及定位，然后再执行post
     * 
     * 此函数在执行时，会变更EntityQuery中的recNo值，确保post能够成功
     * 
     * 注意：如果EntityQuery不存在，则返回-1
     * 
     * @return 返回自身在 EntityQuery 中的序号，从1开始，若没有找到，则返回0
     */
    default int findRecNo() {
        EntityHomeImpl entityHome = getEntityHome();
        if (entityHome != null)
            return entityHome.findRecNo(this);
        else
            return -1;
    }

    /**
     * 从数据集中重新取值
     */
    default void refresh() {
        EntityHomeImpl entityHome = getEntityHome();
        Objects.requireNonNull(entityHome, "entityHome is null");
        entityHome.refresh(this);
    }

    /**
     * 提交到 EntityQuery
     */
    default void post() {
        EntityHomeImpl entityHome = getEntityHome();
        Objects.requireNonNull(entityHome, "entityHome is null");
        entityHome.post(this);
    }

    /**
     * 指定锁的状态是解锁还是锁定
     * 
     * @param flag
     */
    default void setLocked(boolean flag) {

    }

    /**
     * 取得当前锁的启用状态，默认为true
     * 
     * @return boolean
     */
    default boolean isLocked() {
        return true;
    }

    /**
     * 锁定后先进行解锁操作
     */
    default void unlock() {
        setLocked(false);
    }

}
