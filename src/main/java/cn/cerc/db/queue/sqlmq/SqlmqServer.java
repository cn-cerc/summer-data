package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.mysql.MysqlSession;

public class SqlmqServer implements IHandle {
    private static SqlmqServer instance;
    private ISession session;

    private SqlmqServer() {
        SqlmqConfig config = new SqlmqConfig();
        this.setSession(new MysqlSession(config));
    }

    public synchronized static SqlmqServer get() {
        if (instance == null)
            instance = new SqlmqServer();
        return instance;
    }

    public static SqlmqQueue getQueue(String queueId) {
        return new SqlmqQueue(queueId);
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

}
