package cn.cerc.core;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

@Slf4j
public class LanguageResource {

    public static final String LANGUAGE_EN = "en";
    public static final String LANGUAGE_CN = "cn";
    public static final String LANGUAGE_TW = "tw";
    public static final String LANGUAGE_SG = "sg";

    private static String appLanguage = "en";
    private String userLanguage;
    private String resourceFileName;
    private static final Properties resourceProperties = new Properties();

    static {
        final String configFileName = "/application.properties";
        Properties config = new Properties();
        try {
            final InputStream configFile = LanguageResource.class.getResourceAsStream(configFileName);
            if (configFile != null) {
                config.load(configFile);
                log.info("read file: {}", configFileName);
            } else {
                log.warn("{} does not exist.", configFileName);
            }
            appLanguage = config.getProperty("currentLanguage", appLanguage);
            log.info("currentLanguage value: {}", appLanguage);
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {}", configFileName);
        }
    }

    public LanguageResource(String projectId) {
        initResource(projectId, appLanguage);
    }

    public LanguageResource(String projectId, String userLanguage) {
        initResource(projectId, userLanguage);
    }

    private void initResource(String projectId, String userLanguage) {
        log.info("userLanguage {}", userLanguage);
        this.userLanguage = userLanguage;// 用户级的语言类型
        if (Utils.isEmpty(userLanguage)) {
            userLanguage = appLanguage;// 若用户没有传入语言类型则默认选择英文文件
        }
        String resourceFileName = String.format("/%s-%s.properties", projectId, userLanguage);
        try {
            InputStream resourceString = LanguageResource.class.getResourceAsStream(resourceFileName);
            if (resourceString == null) {
                resourceFileName = String.format("/%s.properties", projectId);
                resourceString = LanguageResource.class.getResourceAsStream(resourceFileName);
            }
            if (resourceString != null) {
                this.resourceFileName = resourceFileName;
                log.info("load properties file {}", resourceFileName);
                resourceProperties.load(new InputStreamReader(resourceString, StandardCharsets.UTF_8));
            } else {
                log.warn("{} does not exist.", resourceFileName);
            }
        } catch (IOException e) {
            log.error("Failed to load the settings from the file: {} ", resourceFileName);
        }
    }

    public String getString(String key, String text) {
        if (!resourceProperties.containsKey(key)) {
            log.error("resourceFileName {}, appLanguage {}, userLanguage {}, resource key {}, does not exist.", resourceFileName, appLanguage, userLanguage, key);
        }
        String value = resourceProperties.getProperty(key, text);
        log.info("resourceFileName {}, appLanguage {}, userLanguage {}, key {}, input {}, output {}", resourceFileName, appLanguage, userLanguage, key, text, value);
        return value;
    }

    public String getCurrentLanguage() {
        return appLanguage;
    }

    public static boolean isLanguageTW() {
        return LANGUAGE_TW.equals(appLanguage);
    }

    public static boolean isLanguageCN() {
        return LANGUAGE_CN.equals(appLanguage);
    }

    public static boolean isLanguageSG() {
        return LANGUAGE_SG.equals(appLanguage);
    }

    public static boolean isLanguageEN() {
        return LANGUAGE_EN.equals(appLanguage);
    }

    public static void debugList(Class<?> clazz) {
        int i = 1;
        String key = String.format("%s.%d", clazz.getName(), i);
        while (resourceProperties.containsKey(key)) {
            System.out.println(String.format("%s=%s", key, resourceProperties.getProperty(key)));
            i++;
            key = String.format("%s.%d", clazz.getName(), i);
        }
    }

    public static void main(String[] args) {
        LanguageResource.debugList(DataSet.class);
    }

}
