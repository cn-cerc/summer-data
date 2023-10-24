package cn.cerc.db.log;

import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import cn.cerc.mis.log.ApplicationEnvironment;

public class ApplicationEnvironmentTest {

    @Test
    public void testHostIP() {
        String hostIP = ApplicationEnvironment.hostIP();
        assertNotEquals("127.0.0.1", hostIP);
    }

}
