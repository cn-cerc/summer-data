package cn.cerc.db.core;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.mssql.MssqlServer;
import cn.cerc.db.mysql.MysqlServerMaster;
import cn.cerc.db.oss.OssConnection;

public class StubDatabaseSession implements ISession {
    private static final Logger log = LoggerFactory.getLogger(StubDatabaseSession.class);

    private MysqlServerMaster mysql;
    private MssqlServer mssql;
//    private MongoConfig mgConn;
//    private QueueServer queConn;
    private OssConnection ossConn;

    public StubDatabaseSession() {
        mysql = new MysqlServerMaster();
        mssql = new MssqlServer();
//        mgConn = new MongoConfig();
//        queConn = new QueueServer();
        ossConn = new OssConnection();
    }

    @Override
    public String getCorpNo() {
        throw new RuntimeException("corpNo is null");
    }

    @Override
    public String getUserCode() {
        throw new RuntimeException("userCode is null");
    }

    @Override
    public Object getProperty(String key) {
        if (MysqlServerMaster.SessionId.equals(key))
            return mysql;
        if (MssqlServer.SessionId.equals(key)) {
            return mssql;
        }
        if (OssConnection.sessionId.equals(key))
            return ossConn;
        return null;
    }

    // 用户姓名
    @Override
    public String getUserName() {
        return getUserCode();
    }

    // 设置自定义参数
    @Override
    public void setProperty(String key, Object value) {
        throw new RuntimeException("调用了未被实现的接口");
    }

    // 返回当前是否为已登入状态
    @Override
    public boolean logon() {
        return false;
    }

    @Override
    public void close() {
        if (mysql != null) {
            try {
                mysql.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            mysql = null;
        }
    }

    @Override
    public boolean loadToken(String token) {
        throw new RuntimeException("not support loadToken");
    }

    @Override
    public HttpServletRequest getRequest() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setRequest(HttpServletRequest request) {
        // TODO Auto-generated method stub

    }

    @Override
    public HttpServletResponse getResponse() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setResponse(HttpServletResponse response) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getPermissions() {
        return null;
    }

}
