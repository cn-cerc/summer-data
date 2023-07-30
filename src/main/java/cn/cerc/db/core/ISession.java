package cn.cerc.db.core;

public interface ISession extends SessionImpl, AutoCloseable {

    String INDUSTRY = "industry";// 行业
    String EDITION = "edition";
    String CORP_NO = "corp_no";
    String USER_CODE = "user_code";
    String USER_ID = "user_id";
    String USER_NAME = "user_name";
    String VERSION = "version";

    String CLIENT_DEVICE = "device"; // client device
    String CLIENT_ID = "CLIENTID";// deviceId, machineCode 表示同一个设备码栏位
    String PKG_ID = "pkgId";// pkgId, 手机APP包名
    //
    String COOKIE_ID = "cookie_id"; // cookie id 参数变量
    String LOGIN_SERVER = "login_server";
    String LANGUAGE_ID = "language";

    String TOKEN = "sid"; // session id 参数变量

    String REQUEST = "request";

    // 关闭开启的资源
    @Override
    void close();

}
