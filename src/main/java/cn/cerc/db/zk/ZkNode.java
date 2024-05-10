package cn.cerc.db.zk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

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
    private static final Map<String, String> items = new ConcurrentHashMap<>();
    private static final ZkServer server;
    private static ZkNode instance;

    static {
        server = ZkServer.get();
        rootPath = String.format("/%s/%s/%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion(),
                ServerConfig.getAppOriginal());
    }

    public static ZkNode get() {
        if (instance == null)
            instance = new ZkNode();
        return instance;
    }

    private ZkNode() {
        ZkNode.instance = this;
    }

    public String getString(String key, Supplier<String> supplier) {
        return getNodeValue(rootPath + "/" + key, supplier);
    }

    public String getNodeValue(String node, Supplier<String> supplier) {
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
            String def = supplier.get();
            server.create(node, def, CreateMode.PERSISTENT);
            server.watch(node, this);
            items.put(node, def);
            return def;
        }
    }

    public String getString(String key, String def) {
        return getNodeValue(rootPath + "/" + key, () -> def);
    }

    public int getInt(String key, int def) {
        String value = getNodeValue(rootPath + "/" + key, () -> "" + def);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.error(e.getMessage(), e);
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
                log.debug("节点 {} 值变更为 {}", node, value);
                server.watch(node, this); // 继续监视
            } else {
                log.error("节点 {} 不应该找不到！！！", node);
            }
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            items.remove(node);
            log.debug("节点 {} 已被删除！", node);
        }
    }

    // 使用 ZkServer.get()
    @Deprecated
    public ZkServer server() {
        return server;
    }

    public String rootPath() {
        return rootPath;
    }
}
