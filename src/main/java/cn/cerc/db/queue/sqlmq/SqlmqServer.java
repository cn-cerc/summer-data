package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.mysql.MysqlSession;

/**
 * 临时解决方案：MySQL 的系统参数 wait_timeout 长度改为 15天=1296000秒 <br>
 * 最终解决方案：MySQL 支持断线自动重连机制 HikariCP 启用
 */
public class SqlmqServer implements IHandle {
    private static SqlmqServer instance;
    private ISession session;

    private SqlmqServer() {
        SqlmqConfig config = new SqlmqConfig();
        this.setSession(new MysqlSession(config));
    }

    public static SqlmqServer get() {
        if (instance == null) {
            synchronized (SqlmqServer.class) {
                if (instance == null)
                    instance = new SqlmqServer();
            }
        }
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
