package cn.cerc.db.core;

import java.sql.ResultSet;
import java.sql.Statement;

import javax.servlet.http.HttpServletRequest;

import cn.cerc.db.mysql.MysqlClient;
import cn.cerc.db.mysql.MysqlServerMaster;

public interface IHandle {

    ISession getSession();

    void setSession(ISession session);

    /**
     * 获取行业代码
     */
    default String getIndustry() {
        return getSession().getIndustry();
    }

    /**
     * 获取帐套代码
     */
    default String getCorpNo() {
        return getSession().getCorpNo();
    }

    /**
     * 获取用户代码
     */
    default String getUserCode() {
        return getSession().getUserCode();
    }

    /**
     * 获取用户角色
     */
    default String getUserRole() {
        return getSession().getUserRole();
    }

    /**
     * 允许编辑菜单
     */
    default boolean allowVisualDesign() {
        return getSession().allowVisualDesign();
    }

    default MysqlServerMaster getMysql() {
        return (MysqlServerMaster) getSession().getProperty(MysqlServerMaster.SessionId);
    }

    default HttpServletRequest getRequest() {
        return (HttpServletRequest) getSession().getProperty(ISession.REQUEST);
    }

    /**
     * 若执行sql指令后，有返回一条或一条记录以上，则为true，否则为false;
     * 
     * @param sql sql执行语句
     * 
     * @return database exit
     */
    default boolean DBExists(String sql) {
        try (MysqlClient client = getMysql().getClient()) {
            try (Statement st = client.getConnection().createStatement()) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    return rs.next();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

//    @Deprecated
//    default void setProperty(String key, Object value) {
//        getSession().setProperty(key, value);
//    }

//    @Deprecated
//    default void setHandle(IHandle handle) {
//        this.setSession(handle.getSession());
//    }
}
