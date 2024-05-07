package cn.cerc.db.maintain;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkServer;

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
            ZkServer.get().create(node, "", CreateMode.PERSISTENT);
        }
        String value = ZkServer.get().getValue(node);
        if (Utils.isNotEmpty(value))
            this.timestamp = Long.parseLong(value);
        else
            this.timestamp = 0L;
        ZkServer.get().watch(node, this);// 监听节点
    }

    public String node() {
        return this.node;
    }

    /**
     * 开始停机维护
     */
    public void shutdown() {
        ZkServer.get().setValue(this.node, String.valueOf(System.currentTimeMillis()), CreateMode.PERSISTENT);
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
        if (this.isTerminated())
            return (System.currentTimeMillis() - this.timestamp) > TimeUnit.SECONDS.toMillis(3);
        else
            return false;
    }

    /**
     * 15秒之外，非法消费
     */
    public boolean illegalConsume() {
        if (this.isTerminated())
            return (System.currentTimeMillis() - this.timestamp) > TimeUnit.SECONDS.toMillis(15);
        else
            return false;
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
            if (Utils.isNotEmpty(value))
                this.timestamp = Long.parseLong(value);
            else
                this.timestamp = 0L;
        }

        // 监听节点 /singleton/menu/clear
        ZkServer.get().watch(node, this);
    }

}
