package cn.cerc.db.core;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface SessionImpl {

    // 自定义参数，注：若key=null则返回实现接口的对象本身
    Object getProperty(String key);

    // 设置自定义参数
    void setProperty(String key, Object value);

    // 从数据库根据token载入所有环境
    void loadToken(String token);

    default String getToken() {
        return (String) getProperty(ISession.TOKEN);
    }

    default String getIndustry() {
        return (String) getProperty(ISession.INDUSTRY);
    }

    default String getCorpNo() {
        return (String) getProperty(ISession.CORP_NO);
    }

    default String getUserCode() {
        return (String) getProperty(ISession.USER_CODE);
    }

    default String getEdition() {
        return (String) getProperty(ISession.EDITION);
    }

    default String getUserName() {
        return (String) getProperty(ISession.USER_NAME);
    }

    default String getVersion() {
        return (String) getProperty(ISession.VERSION);
    }

    default String getLanguageId() {
        return (String) getProperty(ISession.LANGUAGE_ID);
    }

    default String getClientDevice() {
        return (String) getProperty(ISession.CLIENT_DEVICE);
    }

    default String getLoginServer() {
        return (String) getProperty(ISession.LOGIN_SERVER);
    }

    // 返回当前是否为已登入状态
    default boolean logon() {
        return false;
    }

    /**
     * 警告：若授权码清单中包含了 Permission.ADMIN，则意味着拥有所有权限！
     * 
     * @return 返回当前用户已取得的授权码清单，若返回null则表示不判断，返回空字符串则等于Permission.USERS
     */
    default String getPermissions() {
        return null;
    }

    default HttpServletRequest getRequest() {
        return null;
    }

    default void setRequest(HttpServletRequest request) {
    }

    default HttpServletResponse getResponse() {
        return null;
    }

    default void setResponse(HttpServletResponse response) {
    }

    default void atSystemUser() {
        throw new RuntimeException("not support atSystemUser");
    }

}
