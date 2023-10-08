package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.mysql.MysqlSession;

public class SqlmqServer implements IHandle {
    private ISession session;

    private SqlmqServer() {
        SqlmqConfig config = new SqlmqConfig();
        this.setSession(new MysqlSession(config));
    }

    public static SqlmqServer get() {
        return new SqlmqServer();
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
