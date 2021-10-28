package cn.cerc.core;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface ISession extends AutoCloseable {
    String TOKEN = "sid"; // session id
    String EDITION = "edition";
    String CORP_NO = "corp_no";
    String USER_CODE = "user_code";
    String USER_NAME = "user_name";
    String VERSION = "version";
    String LANGUAGE_ID = "language_id";
    String CLIENT_DEVICE = "device"; // client device
    String LOGIN_SERVER = "login_server";
    String REQUEST = "request";

    // 自定义参数，注：若key=null则返回实现接口的对象本身
    Object getProperty(String key);

    // 设置自定义参数
    void setProperty(String key, Object value);

    // 从数据库根据token载入所有环境
    void loadToken(String token);

    default String getToken() {
        return (String) getProperty(TOKEN);
    }

    default String getEdition() {
        return (String) getProperty(EDITION);
    }

    default String getCorpNo() {
        return (String) getProperty(CORP_NO);
    }

    default String getUserCode() {
        return (String) getProperty(USER_CODE);
    }

    default String getUserName() {
        return (String) getProperty(USER_NAME);
    }

    default String getVersion() {
        return (String) getProperty(VERSION);
    }

    default String getLanguageId() {
        return (String) getProperty(LANGUAGE_ID);
    }

    default String getClientDevice() {
        return (String) getProperty(CLIENT_DEVICE);
    }

    default String getLoginServer() {
        return (String) getProperty(LOGIN_SERVER);
    }

    // 返回当前是否为已登入状态
    boolean logon();

    // 关闭开启的资源
    @Override
    void close();

    /**
     * 警告：若授权码清单中包含了 Permission.ADMIN，则意味着拥有所有权限！
     * 
     * @return 返回当前用户已取得的授权码清单，若返回null则表示不判断，返回空字符串则等于Permission.USERS
     */
    String getPermissions();

    HttpServletRequest getRequest();

    void setRequest(HttpServletRequest request);

    HttpServletResponse getResponse();

    void setResponse(HttpServletResponse response);
}
