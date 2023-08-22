package cn.cerc.local.tool;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonToolTest {

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

        LinkedHashMap<String, String> map = JsonTool.fromJson(value, mapType);
        map.forEach((k, v) -> log.info("{} {}", k, v));
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