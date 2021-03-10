package cn.cerc.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassConfig {
    private static final Map<String, Properties> buff = new HashMap<>();
    private static final String CONFIGFILE_APPLICATION = "/application.properties";
    private static final Properties configApplication = new Properties();
    private Properties configPackage;
    private String classPath = null;

    static {
        try {
            InputStream file = ClassConfig.class.getResourceAsStream(CONFIGFILE_APPLICATION);
            if (file != null) {
                configApplication.load(new InputStreamReader(file, StandardCharsets.UTF_8));
                log.info("{} is loaded.", CONFIGFILE_APPLICATION);
            } else {
                log.warn("{} doesn't exist.", CONFIGFILE_APPLICATION);
            }
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", CONFIGFILE_APPLICATION);
        }
    }

    public ClassConfig() {
    }

    public ClassConfig(Object owner, String packageName) {
        if (owner instanceof Class)
            this.classPath = ((Class<?>) owner).getName();
        else
            this.classPath = owner.getClass().getName();
        init(packageName);
    }

    private void init(String packageName) {
        if (packageName == null)
            return;

        String configFileName = String.format("/%s.properties", packageName);
        configPackage = buff.get(packageName);
        if (configPackage != null)
            return;

        configPackage = new Properties();
        if (buff.putIfAbsent(packageName, configPackage) != null)
            return;

        try {
            final InputStream configFile = ClassConfig.class.getResourceAsStream(configFileName);
            if (configFile != null) {
                configPackage.load(new InputStreamReader(configFile, StandardCharsets.UTF_8));
                log.info("{} is loaded.", configFileName);
            } else {
                log.warn("{} doesn't exist.", configFileName);
            }
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", configFileName);
        }
    }

    /**
     * 读取以自动加上类名开头的数据
     * 
     * @param key          例如 cn.cerc.core.ClassConfig.1
     * @param defaultValue
     * @return
     */
    public String getString(String key, String defaultValue) {
        if (classPath == null) {
            log.warn("classPath is null.");
            return getValue(key, defaultValue);
        }
        return getValue(String.format("%s.%s", this.classPath, key), defaultValue);
    }

    /**
     * 直接读取文件底层数据，尽量改使用 getString
     * 
     * @param key          例如 app.language
     * @param defaultValue
     * @return
     */
    public String getValue(String key, String defaultValue) {
        log.debug("key: {}", key);
        if (configApplication.containsKey(key))
            return configApplication.getProperty(key);
        else if (configPackage != null)
            return configPackage.getProperty(key, defaultValue);
        else
            return defaultValue;
    }

    public static void main(String[] args) {
        ClassConfig config = new ClassConfig(ClassConfig.class, "summer-core-cn");
        System.out.println(config.getValue("cn.cerc.core.DataSet.1", "not find."));
        System.out.println(config.getValue("app.language", "not find."));
    }
}
