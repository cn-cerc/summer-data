package cn.cerc.db.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class YamlConfig implements IConfig {

    private static final Logger log = LoggerFactory.getLogger(YamlConfig.class);
    private static final Map<String, Object> configMap = new HashMap<>();

    static {
        Yaml yaml = new Yaml();

        // 加载项目文件配置
        String appFile = "/application.yaml";
        InputStream input = ClassConfig.class.getResourceAsStream(appFile);
        if (input != null) {
            configMap.putAll(yaml.loadAs(input, Map.class));
            log.info("{} is loaded.", appFile);
        } else {
            log.warn("{} doesn't exist.", appFile);
        }

        // 加载本地文件配置
        Path localFile = Paths.get(System.getProperty("user.home"), "summer-application.yaml");
        try {
            if (Files.exists(localFile)) {
                configMap.putAll(yaml.loadAs(Files.newInputStream(localFile), Map.class));
                log.info("{} is loaded.", localFile);
            } else {
                log.warn("{} doesn't exist.", localFile);
            }
        } catch (FileNotFoundException e) {
            log.error("The settings file does not exist: {}'", localFile);
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", localFile);
        }
    }

    @Override
    public String getProperty(String key, String def) {
        return getConfigDef(key, def);
    }

    public <T> T getConfigDef(String key, T def) {
        String[] split = key.split("\\.");
        Object result = configMap;
        for (String k : split) {
            if (result == null)
                return def;
            Integer index = null;
            String innerkey = k;
            if (k.contains("[")) {
                int start = k.indexOf("[");
                index = Integer.valueOf(k.substring(start + 1, k.indexOf("]")));
                innerkey = k.substring(0, start);
            }
            if (result instanceof Map) {
                result = ((Map<String, Object>) result).get(innerkey);
            }
            if (index != null && result instanceof List) {
                result = ((ArrayList) result).get(index);
            }
        }
        return (T) result;
    }

}
