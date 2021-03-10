package cn.cerc.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LanguageResource {

    public static final String LANGUAGE_EN = "en";
    public static final String LANGUAGE_CN = "cn";
    public static final String LANGUAGE_TW = "tw";
    public static final String LANGUAGE_SG = "sg";

    public static String appLanguage = "en";
    private String userLanguage;
    private String resourceFileName;
    private static final Properties resourceProperties = new Properties();

    static {
        appLanguage = (new ClassConfig(LanguageResource.class, null)).getValue("app.language", appLanguage);
    }

    public LanguageResource(String projectId) {
        initResource(projectId, appLanguage);
    }

    public LanguageResource(String projectId, String userLanguage) {
        initResource(projectId, userLanguage);
    }

    private void initResource(String projectId, String userLanguage) {
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
            log.error("Failed to load the settings from the file: {}", resourceFileName);
        }
    }

    public String getString(String key, String text) {
        if (!resourceProperties.containsKey(key)) {
            log.error("resourceFileName {}, appLanguage {}, userLanguage {}, resource key {}, does not exist.",
                    resourceFileName, appLanguage, userLanguage, key);
        }
        return resourceProperties.getProperty(key, text);
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
        System.out.println(appLanguage);
    }

}
