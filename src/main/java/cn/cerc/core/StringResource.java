package cn.cerc.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringResource {
    private static String currentTimezone = "en";
    private static final String packageName = "summer-core";
    private static final Properties resourceProperties = new Properties();

    public static String TIMEZONE_EN = "en";
    public static String TIMEZONE_CN = "cn";
    public static String TIMEZONE_TW = "tw";
    public static String TIMEZONE_SG = "sg";

    static {
        final String configFileName = "/application.properties";
        Properties config = new Properties();
        try {
            final InputStream configFile = StringResource.class.getResourceAsStream(configFileName);
            if (configFile != null) {
                config.load(configFile);
                log.info("read file: {}", configFileName);
            } else {
                log.warn("{} does not exist.", configFileName);
            }
            currentTimezone = config.getProperty("currentTimezone", currentTimezone);
            log.info("currentTimezone value: {}", currentTimezone);
            String resourceFileName = String.format("/%s-%s.properties", packageName, currentTimezone);
            try {
                InputStream resourceString = StringResource.class.getResourceAsStream(resourceFileName);
                if (resourceString == null) {
                    resourceFileName = String.format("/%s.properties", packageName);
                    resourceString = StringResource.class.getResourceAsStream(resourceFileName);
                }
                if (resourceString != null) {
                    resourceProperties.load(new InputStreamReader(resourceString, "UTF-8"));
                } else {
                    log.warn("{} does not exist.", resourceFileName);
                }
            } catch (IOException e) {
                log.error("Failed to load the settings from the file: {} ", resourceFileName);
            }
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", configFileName);
        }
    }

    public static String get(Object object, int key, String text) {
        if (object == null) {
            log.error("object is null");
            return text;
        }
        return get(String.format("%s.%d", object.getClass().getName(), key), text);
    }

    public static String get(Class<?> clazz, int key, String text) {
        if (clazz == null) {
            log.error("clazz is null");
            return text;
        }
        return get(String.format("%s.%d", clazz.getName(), key), text);
    }

    private static String get(String key, String text) {
        if (!resourceProperties.containsKey(key))
            log.info("String resource key {} does not exist.", key);
        return resourceProperties.getProperty(key, text);
    }

    public static void list(Class<?> clazz) {
        int i = 1;
        String key = String.format("%s.%d", clazz.getName(), i);
        while (resourceProperties.containsKey(key)) {
            System.out.println(String.format("%s=%s", key, resourceProperties.getProperty(key)));
            i++;
            key = String.format("%s.%d", clazz.getName(), i);
        }
    }

    public static String getCurrentTimezone() {
        return currentTimezone;
    }

    public static void main(String[] args) {
        StringResource.list(DataSet.class);
    }
}
