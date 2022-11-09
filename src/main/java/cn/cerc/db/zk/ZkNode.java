package cn.cerc.db.zk;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import cn.cerc.db.core.ServerConfig;

@Component
public class ZkNode implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZkNode.class);
    private static final String rootPath;
    public static ZkServer server;
    private ConcurrentHashMap<String, String> items = new ConcurrentHashMap<>();
    private static ZkNode instance;

    static {
        rootPath = String.format("/%s/%s/%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion(),
                ServerConfig.getAppIndustry());
        synchronized (ZkConfig.class) {
            if (server == null)
                server = new ZkServer();
        }
    }

    public static ZkNode get() {
        return instance;
    }

    private ZkNode() {
        ZkNode.instance = this;
    }

    public String getString(String key, String def) {
        String node = rootPath + "/" + key;
        if (items.containsKey(node))
            return items.get(node);

        var stat = server.watch(node, this);
        if (stat != null) {
            log.debug("从zeekeeper中读取：{}", node);
            var value = server.getValue(node);
            items.put(node, value);
            return value;
        } else {
            log.debug("在zeekeeper中建立 {}", node);
            server.create(node, def, CreateMode.PERSISTENT);
            server.watch(node, this);
            items.put(node, def);
            return def;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        var node = event.getPath();
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            var value = server.getValue(node);
            if (value != null) {
                items.put(node, value);
                log.debug("{} 变更为：{}", node, value);
                server.watch(node, this); // 继续监视
            } else {
                log.error("{} 不应该找不到！！！", node);
            }
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            items.remove(node);
            log.debug("{} 已被删除！", node);
        }
    }

    public ZkServer server() {
        return server;
    }
}
