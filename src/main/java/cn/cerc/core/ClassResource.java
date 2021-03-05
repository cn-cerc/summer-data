package cn.cerc.core;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ClassResource {
    private static Map<String, LanguageResource> buffer = new HashMap<>();
    private LanguageResource stringResource;
    private String languageId;
    private String classPath;
    private String resourceFile;
    private Object owner;

    public ClassResource(String resourceFile, Class<?> clazz) {
        this.classPath = clazz.getName();
        this.resourceFile = resourceFile;
    }

    public ClassResource(Object owner, String resourceFile) {
        this.owner = owner;
        this.classPath = owner.getClass().getName();
        this.resourceFile = resourceFile;
    }

    public String getString(int id, String string) {
        if (stringResource == null) {
            initResource();
        }
        return stringResource.getString(String.format("%s.%d", this.classPath, id), string);
    }

    private void initResource() {
        // 首次加载语言类型为空不加载，底部默认为空的语言LanguageResource会直接使用currentLanguage
        if (this.languageId != null) {
            stringResource = buffer.get(getBuffKey(resourceFile, this.languageId));
        }

        if (stringResource == null) {
            if (owner != null && owner instanceof IUserLanguage) {
                this.languageId = ((IUserLanguage) owner).getLanguageId();
                log.info(owner.getClass().getName());
            }
            stringResource = new LanguageResource(resourceFile, this.languageId);
            log.info("languageId {}", languageId);
            buffer.put(getBuffKey(resourceFile, this.languageId), stringResource);
        }
    }

    private String getBuffKey(String resourceFile, String languageId) {
        if (Utils.isEmpty(languageId)) {
            return resourceFile;
        } else {
            return String.format("%s-%s", resourceFile, languageId);
        }
    }

    public String getFile(String fileName) {
        if (stringResource == null) {
            initResource();
        }
        return stringResource.getString(String.format("%s.file.%s", this.classPath, fileName), fileName);
    }

}
