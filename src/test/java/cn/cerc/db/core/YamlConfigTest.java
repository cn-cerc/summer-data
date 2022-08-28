package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class YamlConfigTest {

    @Test
    public void test_1() {
        String str = new YamlConfig().getProperty("app.service.path", "");
        assertEquals("service-fpl", str);
    }

    @Test
    public void test_arr() {
        String str = new YamlConfig().getProperty("blackList[2]");
        assertEquals("183.8.87.151", str);
    }

    @Test
    public void test_getMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "service-fpl");
        map.put("token", "0f1e29918bce4b9faef5000e2cca853e");
        Map<String, Object> result = new YamlConfig().getConfigDef("app.service", new HashMap<String, Object>());
        assertEquals(map, result);
    }

    @Test
    public void test_getList() {
        List<String> list = new ArrayList<>();
        list.add("183.8.87.106");
        list.add("183.8.87.125");
        list.add("183.8.87.151");
        list.add("183.8.87.141");
        list.add("183.8.87.102");
        List<String> result = new YamlConfig().getConfigDef("blackList", new ArrayList<String>());
        assertEquals(list, result);
    }

}
