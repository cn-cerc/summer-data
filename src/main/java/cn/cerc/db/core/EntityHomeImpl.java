package cn.cerc.db.core;

public interface EntityHomeImpl {

    /**
     * 提交entity到query
     * 
     * @param entity 要保存的entity对象
     */
    void post(EntityImpl entity);

    /**
     * @param entity 要查找的entity对象
     * @return 返回entity在query中的序号，从1开始，若有找到则变更并返回recNo，否则返回0
     */
    int findRecNo(EntityImpl entity);

    /**
     * 从query中重新给entity赋值
     * 
     * @param entity 要赋值的entity对象
     */
    void refresh(EntityImpl entity);

}
