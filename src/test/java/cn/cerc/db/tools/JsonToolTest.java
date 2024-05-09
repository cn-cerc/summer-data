package cn.cerc.db.tools;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import cn.cerc.local.tool.JsonTool;

public class JsonToolTest {

    private static final Logger log = LoggerFactory.getLogger(JsonToolTest.class);

    @Test
    public void test_json() {
        Map<String, String> items = new LinkedHashMap<>();
        items.put("1", "a");
        items.put("2", "b");
        items.put("3", "c");
        String value = JsonTool.toJson(items);
        log.info(value);

        TypeFactory typeFactory = new ObjectMapper().getTypeFactory();
        MapType mapType = typeFactory.constructMapType(LinkedHashMap.class, String.class, String.class);

        JsonTool.fromJson(value, mapType, () -> new LinkedHashMap<String, String>())
                .forEach((k, v) -> log.info("{} -> {}", k, v));
    }

//    @Test
    public void test_format_01() {
        String json = """
                {"name":"itjun","age":30,"template":{"key1":"value1","key2":"value2"}}
                """;
        String value = JsonTool.format(json);
        assertEquals("""
                {
                  "name" : "itjun",
                  "age" : 30,
                  "template" : {
                    "key1" : "value1",
                    "key2" : "value2"
                  }
                }
                """.trim(), value.trim());
    }

}