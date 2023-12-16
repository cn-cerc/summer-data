package cn.cerc.db.core;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface ISession extends AutoCloseable {

    String INDUSTRY = "industry";// 行业
    String EDITION = "edition";
    String CORP_NO = "corp_no";
    String USER_CODE = "user_code";
    String USER_ROLE = "user_role";// 用户角色

    /**
     * 用户角色
     */
    String users_role = "users_role";
    /**
     * 参考角色
     */
    String refer_role = "refer_role";
    /**
     * 系统角色
     */
    String admin_role = "admin_role";

    String USER_ID = "user_id";
    String USER_NAME = "user_name";
    String SUPER_USER = "super_user";
    String VERSION = "version";

    String CLIENT_DEVICE = "device"; // client device
    String CLIENT_ID = "CLIENTID";// deviceId, machineCode 表示同一个设备码栏位
    String PKG_ID = "pkgId";// pkgId, 手机APP包名

    String COOKIE_ID = "cookie_id"; // cookie id 参数变量
    String LOGIN_SERVER = "login_server";
    String LANGUAGE_ID = "language";

    String TOKEN = "sid"; // session id 参数变量
    String REQUEST = "request";

    // 允许编辑菜单
    String Visual_Design = "visual_design";

    // 自定义参数，注：若key=null则返回实现接口的对象本身
    Object getProperty(String key);

    // 设置自定义参数
    void setProperty(String key, Object value);

    // 从数据库根据token载入所有环境
    boolean loadToken(String token);

    default String getToken() {
        return (String) getProperty(ISession.TOKEN);
    }

    default String getIndustry() {
        return (String) getProperty(ISession.INDUSTRY);
    }

    default boolean allowVisualDesign() {
        return "true".equals(getProperty(ISession.Visual_Design));
    }

    default String getUserRole() {
        return (String) getProperty(ISession.USER_ROLE);
    }

    /**
     * 获取用户角色代码
     */
    default String getUsersRole() {
        return (String) getProperty(ISession.users_role);
    }

    /**
     * 获取标准角色代码
     */
    default String getReferRole() {
        return (String) getProperty(ISession.refer_role);
    }

    /**
     * 获取系统角色代码
     */
    default String getAdminRole() {
        return (String) getProperty(ISession.admin_role);
    }

    default boolean isSuperUser() {
        return "true".equals(getProperty(ISession.SUPER_USER));
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

    // 关闭开启的资源
    @Override
    void close();

}
