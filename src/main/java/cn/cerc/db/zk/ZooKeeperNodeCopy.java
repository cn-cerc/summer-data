package cn.cerc.db.zk;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperNodeCopy {
    private static final String Source = "/4plc/itjun";
    private static final String Target = "/4plc/itjun-alpha";
    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws Exception {
        zooKeeper = ZkServer.get().client();
        copy(Source, Target);
    }

    private static void copy(String source, String target) throws Exception {
        byte[] data = zooKeeper.getData(source, false, null);
        zooKeeper.create(target, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        List<String> children = zooKeeper.getChildren(source, false);
        for (String child : children) {
            String childSourcePath = source + "/" + child;
            String childDestinationPath = target + "/" + child;
            copy(childSourcePath, childDestinationPath);
        }
    }

}
