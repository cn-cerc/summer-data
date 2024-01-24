package cn.cerc.db.mysql;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import cn.cerc.db.core.ISession;
import cn.cerc.db.core.LanguageResource;

/**
 * 同步日瓦数据库专用
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MysqlSession implements ISession {
    private static final Logger log = LoggerFactory.getLogger(MysqlSession.class);

    protected Map<String, Object> connections = new HashMap<>();
    private final Map<String, Object> params = new HashMap<>();
    protected String permissions = null;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private MysqlConfigImpl config;

    public MysqlSession() {
        super();
    }

    public MysqlSession(MysqlConfigImpl config) {
        super();
        this.config = config;
        params.put(ISession.CORP_NO, "");
        params.put(ISession.USER_CODE, "");
        params.put(ISession.USER_NAME, "");
        params.put(ISession.LANGUAGE_ID, LanguageResource.appLanguage);
    }

    @Override
    public final void setProperty(String key, Object value) {
        if (ISession.TOKEN.equals(key)) {
            if ("{}".equals(value)) {
                params.put(key, null);
            } else {
                if (value == null || "".equals(value))
                    params.clear();
                else {
                    params.put(key, value);
                }
            }
            return;
        }
        if (ISession.REQUEST.equals(key))
            this.request = (HttpServletRequest) value;
        params.put(key, value);
    }

    @Override
    public final Object getProperty(String key) {
        if (key == null)
            return this;

        Object result = null;
        if (params.containsKey(key)) {
            result = params.get(key);
            if (result != null)
                return result;
        }

        if (connections.containsKey(key)) {
            result = connections.get(key);
            if (result != null)
                return result;
        }

        if (MysqlServerMaster.SessionId.equals(key)) {
            MysqlServerCustom obj = new MysqlServerCustom(config);
            connections.put(MysqlServerMaster.SessionId, obj);
            return connections.get(key);
        }
        return null;
    }

    @Override
    public void close() {
        for (String key : this.connections.keySet()) {
            Object sess = this.connections.get(key);
            try {
                if (sess instanceof AutoCloseable) {
                    ((AutoCloseable) sess).close();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        connections.clear();
    }

    @Override
    public String getCorpNo() {
        return (String) this.getProperty(ISession.CORP_NO);
    }

    @Override
    public String getUserCode() {
        return (String) this.getProperty(ISession.USER_CODE);
    }

    @Override
    public final String getUserName() {
        return (String) this.getProperty(ISession.USER_NAME);
    }

    @Override
    public boolean logon() {
        return this.getProperty(ISession.TOKEN) != null;
    }

    @Override
    public boolean loadToken(String token) {
        return true;
    }

    @Override
    public final String getPermissions() {
        return this.permissions;
    }

    @Override
    public HttpServletRequest getRequest() {
        return this.request;
    }

    @Override
    public void setRequest(HttpServletRequest request) {
        this.request = request;
    }

    @Override
    public HttpServletResponse getResponse() {
        return this.response;
    }

    @Override
    public void setResponse(HttpServletResponse response) {
        this.response = response;
    }

    public static class MysqlServerCustom extends MysqlServer {
        private MysqlConfigImpl mysqlConfig;

        public MysqlServerCustom(MysqlConfigImpl config) {
            this.mysqlConfig = config;
        }

        @Override
        public Connection createConnection() {
            // 不使用线程池直接创建
            if (getConnection() == null) {
                MysqlConfig master = MysqlConfig.getMaster();
                setConnection(master.createConnection(this.getHost(), this.getDatabase(), mysqlConfig.getUsername(),
                        mysqlConfig.getPassword()));
            }
            return getConnection();
        }

        @Override
        public final boolean isPool() {
            return false;
        }

        @Override
        public String getHost() {
            return mysqlConfig.getHost();
        }

        @Override
        public String getDatabase() {
            return mysqlConfig.getDatabase();
        }

    }
}
