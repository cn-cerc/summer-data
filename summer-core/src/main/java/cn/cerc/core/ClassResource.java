package cn.cerc.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 应用级别的语言资源文件
 */
public class ClassResource {
    private static Map<String, LanguageResource> buffer = new ConcurrentHashMap<>();
    private LanguageResource languageResource;
    private String classPath;
    private String projectId;

    public ClassResource(Class<?> owner, String projectId) {
        this.classPath = ((Class<?>) owner).getName();
        this.projectId = projectId;
    }

    public String getString(int it, String defaultValue) {
        if (languageResource == null) {
            initResource();
        }
        return languageResource.getString(String.format("%s.%d", this.classPath, it), defaultValue);
    }

    public String getFile(String fileName) {
        if (languageResource == null) {
            initResource();
        }
        return languageResource.getString(String.format("%s.file.%s", this.classPath, fileName), fileName);
    }

    private void initResource() {
        languageResource = new LanguageResource(projectId, LanguageResource.appLanguage);
        buffer.put(getBuffKey(projectId, LanguageResource.appLanguage), languageResource);
    }

    private String getBuffKey(String resourceFile, String languageId) {
        if (Utils.isEmpty(languageId)) {
            return resourceFile;
        } else {
            return String.format("%s-%s", resourceFile, languageId);
        }
    }
}
