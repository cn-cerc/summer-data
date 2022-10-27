package cn.cerc.db.zk;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ZkConfigTest {

    @Test
    public void testBuildKey() {
        assertEquals("/a", ZkConfig.createKey(null, "a"));
        assertEquals("/a", ZkConfig.createKey("", "a"));
        assertEquals("/a", ZkConfig.createKey("/", "a"));
        //
        assertEquals("/pa/a", ZkConfig.createKey("/pa", "a"));
        assertEquals("/pa/a", ZkConfig.createKey("/pa/", "a"));
    }

}
