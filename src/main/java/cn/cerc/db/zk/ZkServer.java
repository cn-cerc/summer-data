package cn.cerc.db.zk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

public class ZkServer {
    private static final Logger log = LoggerFactory.getLogger(ZkServer.class);
    private static final ZkServer instance = new ZkServer();
    /**
     * 会话过期时间
     * <p>
     * 在客户端与服务端断开连接后，如果在会话过期时间内没有重连上那么临时节点将会消失
     * <p>
     * 客户端断开连接会自动尝试重连
     */
    private static final int SESSION_TIMEOUT = 10000;
    private final Set<WatcherRecord> watchers = Collections.synchronizedSet(new HashSet<>());
    private final Map<String, EphemeralNodeRecord> ephemeralNodeMap = new ConcurrentHashMap<>();
    private String host;
    private ZooKeeper client;
    private long sessionId = -1;
    private ZkSessionWatcher sessionWatcher;

    public static ZkServer get() {
        return instance;
    }

    private ZkServer() {
        System.setProperty("zookeeper.sasl.client", "false");

        // 私有构造函数，防止外部实例化
        ServerConfig config = ServerConfig.getInstance();
        String host = config.getProperty("zookeeper.host");
        String port = config.getProperty("zookeeper.port", "2181");
        if (Utils.isEmpty(host)) {
            log.error("严重错误：读取不到 zookeeper.host 配置项！");
            return;
        }
        if (!host.contains(":"))
            host = host + ":" + port;
        this.host = host;
        this.connection();
    }

    public ZkServer connection() {
        try {
            if (this.client != null && this.client.getState().isConnected())
                return this;
            synchronized (this) {
                if (this.client != null && this.client.getState().isConnected())
                    return this;

                this.sessionWatcher = new ZkSessionWatcher(this, new CountDownLatch(1));
                this.client = new ZooKeeper(host, SESSION_TIMEOUT, sessionWatcher);
                if (sessionWatcher.await(60, TimeUnit.SECONDS)) { // 等待zk联接成功
                    long sessionId = this.client.getSessionId();
                    if (this.sessionId != sessionId) {
                        // 重建临时节点
                        for (String node : ephemeralNodeMap.keySet()) {
                            EphemeralNodeRecord record = ephemeralNodeMap.get(node);
                            try {
                                this.client.create(node, record.value().getBytes(), Ids.OPEN_ACL_UNSAFE,
                                        record.createMode());
                                log.info("重建临时节点 {} 成功", node);
                            } catch (KeeperException | InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                        // 重建观察者
                        for (WatcherRecord record : watchers) {
                            try {
                                if (record.root()) {
                                    this.client.getChildren(record.node(), record.watcher());
                                } else {
                                    this.client.exists(record.node(), record.watcher());
                                }
                                log.info("重建观察者 {} 成功", record.node());
                            } catch (KeeperException | InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                        this.sessionId = sessionId;
                    }
                } else {
                    log.error("Zookeeper 连接超时");
                }
            }
        } catch (IOException | InterruptedException e) {
            log.error("{} {}", host, e.getMessage(), e);
        }
        return this;
    }

    private record ZkSessionWatcher(ZkServer server, CountDownLatch latch) implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == EventType.None) {
                latch.countDown();
                States state = server.client.getState();
                String sessionId = Long.toHexString(server.client.getSessionId());
                if (event.getState() == KeeperState.SyncConnected) {
                    log.info("ZooKeeper 0x{} 已接入 {} state {}", sessionId, server.host, state);
                } else if (event.getState() == KeeperState.Disconnected) {
                    // 客户端断开连接，客户端会自动尝试重新连接
                    log.warn("Zookeeper 0x{} 已断开连接 {} state {}", sessionId, server.host, state);
                } else if (event.getState() == KeeperState.Closed) {
                    // 客户端调用 close() 方法时触发
                    log.warn("ZooKeeper 0x{} 已关闭 {} state {}", sessionId, server.host, state);
                } else if (event.getState() == KeeperState.Expired) {
                    // 客户端在 会话过期时间 内未能重新连接服务端
                    log.warn("ZooKeeper 0x{} 会话过期 {} state {}", sessionId, server.host, state);
                } else {
                    log.error("ZooKeeper 0x{} 未处理事件 {} state {}", sessionId, event.getState().name(), state);
                }
            }
        }

        public boolean await(long time, TimeUnit timeUnit) throws InterruptedException {
            return latch.await(time, timeUnit);
        }
    }

    /**
     * 应该改为 private 禁止外部调用，由该类提供方法进行调用
     * 
     * @return 返回Zookeeper连接对象，不要使用变量保存！zookeeper连接有可能会断开
     */
    public ZooKeeper client() {
        if (this.sessionWatcher == null)
            throw new RuntimeException("ZooKeeper 未连接");
        try {
            if (this.sessionWatcher.await(60, TimeUnit.SECONDS)) {
                if (this.client != null && this.client.getState().isConnected())
                    return this.client;
                else {
                    log.warn("ZooKeeper 连接中断，尝试重新连接");
                    return connection().client();
                }
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        throw new RuntimeException("ZooKeeper 未连接");
    }

    /**
     * @return 返回创建的节点名称
     */
    public String create(String path, String value, CreateMode createMode) {
        int site = path.lastIndexOf("/");
        if (site > 0) {
            String parent = path.substring(0, site);
            if (!this.exists(parent))
                this.create(parent, "", CreateMode.PERSISTENT);
        }

        try {
            log.info("create node {}", path);
            // 参数：1，节点路径； 2，要存储的数据； 3，节点的权限； 4，节点的类型
            String result = client().create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE, createMode);
            if (createMode.isEphemeral()) // 如果创建的是临时节点
                ephemeralNodeMap.put(path, new EphemeralNodeRecord(path, value, createMode));
            return result;
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, path, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 删除节点
     *
     * @return 成功否
     */
    public boolean delete(String path) {
        try {
            Stat stat = client().exists(path, false);
            if (stat != null) {
                client().delete(path, stat.getVersion());
                return true;
            } else
                return false;
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, path, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 判断节点是否存在
     *
     * @return 存在否
     */
    public boolean exists(String node) {
        return this.watch(node, null) != null;
    }

    public Stat watch(String node, Watcher watcher) {
        if (Utils.isEmpty(node))
            return null;
        try {
            if (watcher == null) {
                return client().exists(node, false);
            } else {
                Stat stat = client().exists(node, watcher);
                watchers.add(new WatcherRecord(node, watcher, false));
                return stat;
            }
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, node, e.getMessage(), e);
            return null;
        }
    }

    public void watchRoot(String rootPath, Watcher watcher) {
        if (Utils.isEmpty(rootPath))
            return;
        try {
            if (watcher != null) {
                client().getChildren(rootPath, watcher);
                watchers.add(new WatcherRecord(rootPath, watcher, true));
            }
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, rootPath, e.getMessage(), e);
        }
    }

    /**
     * @return 返回所有的子节点
     */
    public List<String> getNodes(String node) {
        try {
            return client().getChildren(node, false);
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, node, e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * @return 取得指点节点的值，若不存在则为null
     */
    public String getValue(String node) {
        try {
            Stat stat = client().exists(node, false);
            if (stat != null)
                return new String(client().getData(node, false, stat), StandardCharsets.UTF_8);
            else {
                log.warn("not find node {} ", node);
                return null;
            }
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, node, e.getMessage(), e);
            return null;
        }
    }

    /**
     * @return 设置指定节点的值
     */
    public ZkServer setValue(String node, String value, CreateMode createMode) {
        try {
            Stat stat = client().exists(node, false);
            if (stat != null) {
                client().setData(node, value.getBytes(), stat.getVersion());
                if (createMode.isEphemeral() && ephemeralNodeMap.containsKey(node)) // 同时更新临时节点的数据
                    ephemeralNodeMap.put(node, new EphemeralNodeRecord(node, value, createMode));
            } else {
                this.create(node, value, createMode);
            }
        } catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            log.error("{} {} {}", host, node, e.getMessage(), e);
        }
        return this;
    }

    public void close() {
        try {
            if (this.client == null || !this.client.getState().isConnected())
                return;
            synchronized (this) {
                if (this.client == null || !this.client.getState().isConnected())
                    return;
                this.client.close();
                log.info("zookeeper client 已关闭");
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    private record EphemeralNodeRecord(String node, String value, CreateMode createMode) {

    }

    private record WatcherRecord(String node, Watcher watcher, boolean root) {

    }

}
