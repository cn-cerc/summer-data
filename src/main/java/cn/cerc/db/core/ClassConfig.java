package cn.cerc.db.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassConfig implements IConfig {
    private static final Logger log = LoggerFactory.getLogger(ClassConfig.class);
    private static final Properties localConfig = new Properties();
    private static final Properties applicationConfig = new Properties();
    private static final Map<String, Properties> buffer = new ConcurrentHashMap<>();
    private final String classPath;
    private final Properties config;

    static {
        // 加载本地文件配置
        Path localFile = Paths.get(System.getProperty("user.home"), "summer-application.properties");
        try {
            if (Files.exists(localFile)) {
                InputStream input = Files.newInputStream(localFile);
                localConfig.load(new InputStreamReader(input, StandardCharsets.UTF_8));
                log.info("{} is loaded.", localFile);
            } else {
                log.warn("{} doesn't exist.", localFile);
            }
        } catch (FileNotFoundException e) {
            log.error("The settings file does not exist: {}'", localFile, e);
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", localFile, e);
        }

        // 加载项目文件配置
        String appFile = "/application.properties";
        try {
            InputStream input = ClassConfig.class.getResourceAsStream(appFile);
            if (input != null) {
                applicationConfig.load(new InputStreamReader(input, StandardCharsets.UTF_8));
                log.info("{} is loaded.", appFile);
            } else {
                log.warn("{} doesn't exist.", appFile);
            }
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", appFile, e);
        }
    }

    public ClassConfig() {
        super();
        classPath = null;
        config = new Properties();
        config.putAll(applicationConfig);
        config.putAll(localConfig);
    }

    public ClassConfig(Class<?> owner, String packageName) {
        super();
        this.classPath = owner.getName();
        config = new Properties();

        if (packageName == null) {
            config.putAll(applicationConfig);
            config.putAll(localConfig);
            return;
        }

        String configFileName = String.format("/%s.properties", packageName);
        Properties packageConfig = buffer.get(packageName);
        if (packageConfig == null) {
            packageConfig = new Properties(applicationConfig);
            if (buffer.putIfAbsent(packageName, packageConfig) == null) {
                try {
                    final InputStream configFile = ClassConfig.class.getResourceAsStream(configFileName);
                    if (configFile != null) {
                        packageConfig.load(new InputStreamReader(configFile, StandardCharsets.UTF_8));
                        log.debug("{} is loaded.", configFileName);
                    } else {
                        log.warn("{} doesn't exist.", configFileName);
                    }
                } catch (IOException e) {
                    log.error("Failed to load the settings from the file: {}", configFileName);
                }
            } else {
                packageConfig = buffer.get(packageName);
            }
        }
        config.putAll(packageConfig);
        config.putAll(applicationConfig);
        config.putAll(localConfig);
    }

    /**
     * 读取配置文件数据，不会自动追加class路径，否则请使用 getClassValue
     *
     * @param key          例如 app.language
     * @param defaultValue 定义默认值
     * @return 返回值，可为null
     */
    @Override
    public String getProperty(String key, String defaultValue) {
        return config.getProperty(key, defaultValue);
    }

    public String getString(String key, String defaultValue) {
        return getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(getProperty(key, String.valueOf(defaultValue)));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = getProperty(key, String.valueOf(defaultValue));
        if ("1".equals(value)) {
            log.warn("key {} config old, Please up to use true/false", key);
            return true;
        }
        return "true".equals(value);
    }

    /**
     * 读取以自动加上类名开头的数据
     *
     * @param key          例如 cn.cerc.core.ClassConfig.1
     * @param defaultValue 定义默认值
     * @return 返回值，可为null
     */
    public String getClassProperty(String key, String defaultValue) {
        if (classPath == null)
            throw new RuntimeException("classPath is null.");
        return getProperty(String.format("config.%s.%s", this.classPath, key), defaultValue);
    }

    public Properties getProperties() {
        return config;
    }

    public static void main(String[] args) {
        String key = "cn.cerc.core.DataSet.1";
        ClassConfig config = new ClassConfig(ClassConfig.class, "summer-core-cn");
        System.out.println(config.getProperty(key, "not find."));

        ClassConfig.localConfig.setProperty(key, "from summer-application");
        System.out.println(config.getProperty(key, "not find."));

        ClassConfig.applicationConfig.setProperty(key, "from appliation");
        System.out.println(config.getProperty(key, "not find."));
    }

}
