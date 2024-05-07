package cn.cerc.db.maintain;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkServer;

import java.util.concurrent.TimeUnit;

/**
 * 运维检修
 */
public class MaintainConfig implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(MaintainConfig.class);
    private static final MaintainConfig instance = new MaintainConfig();
    private final String node;
    private long timestamp;

    public static MaintainConfig build() {
        return instance;
    }

    private MaintainConfig() {
        // path -> /4plc/alpha/maintain
        node = "/" + String.join("/", ServerConfig.getAppProduct(), ServerConfig.getAppVersion(), "maintain");
        if (!ZkServer.get().exists(node)) {
            String value = new Datetime().toString();
            ZkServer.get().create(node, value, CreateMode.PERSISTENT);
        }
        String value = ZkServer.get().getValue(node);
        this.timestamp = Long.parseLong(value);
        ZkServer.get().watch(node, this);// 监听节点
    }

    /**
     * 开始停机维护
     */
    public void shutdown() {
        ZkServer.get().setValue(node, String.valueOf(System.currentTimeMillis()), CreateMode.PERSISTENT);
    }

    /**
     * 启动系统服务
     */
    public void startup() {
        ZkServer.get().setValue(node, "0", CreateMode.PERSISTENT);
    }

    /**
     * 系统已经停止
     */
    public boolean isTerminated() {
        return this.timestamp > 0L;
    }

    /**
     * 03秒之外，非法生产
     */
    public boolean illegalProduce() {
        return (System.currentTimeMillis() - this.timestamp)  > TimeUnit.SECONDS.toMillis(3);
    }

    /**
     * 15秒之外，非法消费
     */
    public boolean illegalConsume() {
        return System.currentTimeMillis() - this.timestamp > TimeUnit.SECONDS.toMillis(15);
    }

    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (!this.node.equals(path)) {
            // 继续监听节点 /singleton/menu/clear
            ZkServer.get().watch(node, this);
            return;
        }

        if (event.getType() != Event.EventType.NodeDataChanged) {
            // 继续监听节点 /maintain
            ZkServer.get().watch(node, this);
            return;
        }

        if (ZkServer.get().exists(node)) {
            // 获取 /maintain 节点的最新值
            String value = ZkServer.get().getValue(path);
            log.info("{} node changed -> {}", path, value);
            this.timestamp = Long.parseLong(value);
        }

        // 监听节点 /singleton/menu/clear
        ZkServer.get().watch(node, this);
    }

}
