package cn.cerc.db.zk;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

public class ZkServer implements AutoCloseable, Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZkServer.class);
    private static final ZkServer server;
    private CountDownLatch cdl;
    private ZooKeeper client;
    private String host;
    private String port;

    static {
        server = new ZkServer();
    }

    public static ZkServer get() {
        return server;
    }

    private ZkServer() {
        var config = ServerConfig.getInstance();
        this.host = config.getProperty("zookeeper.host");
        this.port = config.getProperty("zookeeper.port", "2181");
        if (Utils.isEmpty(host)) {
            log.error("严重错误：读取不到 zookeeper.host 配置项！");
            return;
        }
        if (!host.contains(":"))
            host = host + ":" + this.port;

        try {
            cdl = new CountDownLatch(1);
            System.setProperty("zookeeper.sasl.client", "false");
            this.client = new ZooKeeper(host, 50000, this);
            cdl.await(60, TimeUnit.SECONDS); // 等待zk联接成功
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public ZooKeeper client() {
        return this.client;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == EventType.None) {
            cdl.countDown();
            if (event.getState() == KeeperState.SyncConnected) {
                log.info("ZooKeeper 成功联接到 " + this.getHost());
            } else if (event.getState() == KeeperState.Closed) {
                log.info("ZooKeeper 关闭联接 " + this.getHost());
            } else
                log.error("未处理事件：" + event.getState().name());
        }
    }

    @Override
    public void close() {
        if (this.client != null) {
            try {
                cdl = new CountDownLatch(1);
                client.close();
                client = null;
                cdl.await();
                Thread.sleep(300);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 
     * @param path
     * @param value
     * @return 返回创建的节点名称
     */
    public String create(String path, String value, CreateMode createMode) {
        var site = path.lastIndexOf("/");
        if (site > 0) {
            var parent = path.substring(0, site);
            if (!this.exists(parent))
                this.create(parent, "", CreateMode.PERSISTENT);
        }

        try {
            log.info("create node: " + path);
            // 参数：1，节点路径； 2，要存储的数据； 3，节点的权限； 4，节点的类型
            return client.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 删除节点
     * 
     * @param path
     * @return 成功否
     */
    public boolean delete(String path) {
        try {
            Stat stat = client.exists(path, false);
            if (stat != null) {
                client.delete(path, stat.getVersion());
                return true;
            } else
                return false;
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 判断节点是否存在
     * 
     * @param node
     * @return 存在否
     */
    public boolean exists(String node) {
        return this.watch(node, null) != null;
    }

    public Stat watch(String node, Watcher watcher) {
        try {
            if (watcher == null)
                return client.exists(node, false);
            else
                return client.exists(node, watcher);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 
     * @param node
     * @return 返回所有的子节点
     */
    public List<String> getNodes(String node) {
        try {
            return client.getChildren(node, false);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * 
     * @param node
     * @return 取得指点节点的值，若不存在则为null
     */
    public String getValue(String node) {
        try {
            Stat stat = client.exists(node, false);
            if (stat != null)
                return new String(client.getData(node, false, stat), "UTF-8");
            else {
                log.warn("not find node: {} ", node);
                return null;
            }
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 
     * @param node
     * @param value
     * @return 设置指定节点的值
     */
    public ZkServer setValue(String node, String value, CreateMode createMode) {
        try {
            Stat stat = client.exists(node, false);
            if (stat != null) {
                client.setData(node, value.getBytes(), stat.getVersion());
            } else
                this.create(node, value, createMode);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return this;
    }

    public String getHost() {
        return host;
    }

}
