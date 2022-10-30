package cn.cerc.db.zk;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

public class ZkServer implements AutoCloseable, Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZkServer.class);
    private CountDownLatch cdl;
    private ZooKeeper client;
    private String host;

    public ZkServer() {
        this.host = ServerConfig.getInstance().getProperty("zookeeper.host");
        if (host == null) {
            log.error("严重错误：读取不到 zookeeper.host 配置项！");
            return;
        }
        try {
            cdl = new CountDownLatch(1);
            this.client = new ZooKeeper(host, 15000, this);
            cdl.await(); // 等待zk联接成功
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
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
                System.out.println("未处理事件：" + event.getState().name());
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
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 
     * @param path
     * @param value
     * @return 返回创建的节点名称
     */
    public String create(String path, String value) {
        try {
            // 参数：1，节点路径； 2，要存储的数据； 3，节点的权限； 4，节点的类型
            return client.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
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
            log.error(e.getMessage());
            e.printStackTrace();
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
        try {
            Stat stat = client.exists(node, false);
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return false;
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
            log.error(e.getMessage());
            e.printStackTrace();
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
            else
                return null;
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 
     * @param node
     * @param value
     * @return 设置指定节点的值
     */
    public ZkServer setValue(String node, String value) {
        try {
            Stat stat = client.exists(node, false);
            if (stat != null) {
                client.setData(node, value.getBytes(), stat.getVersion());
            } else
                this.create(node, value);
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
