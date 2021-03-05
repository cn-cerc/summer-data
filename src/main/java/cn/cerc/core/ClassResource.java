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

    public ClassResource(String resourceFile, Object owner) {
        this.owner = owner;
        this.classPath = owner.getClass().getName();
        this.resourceFile = resourceFile;
    }

    private void initResource() {
        stringResource = buffer.get(String.format("%s-%s", resourceFile, this.languageId));
        if (stringResource == null) {
            if (owner != null && owner instanceof IUserLanguage) {
                this.languageId = ((IUserLanguage) owner).getLanguageId();
            }
            stringResource = new LanguageResource(resourceFile, this.languageId);
            buffer.put(String.format("%s-%s", resourceFile, stringResource.getCurrentLanguage()), stringResource);
        }
    }

    public String getString(int id, String string) {
        if (stringResource == null) {
            initResource();
        }
        return stringResource.getString(String.format("%s.%d", this.classPath, id), string);
    }

    public String getFile(String fileName) {
        if (stringResource == null) {
            initResource();
        }
        return stringResource.getString(String.format("%s.file.%s", this.classPath, fileName), fileName);
    }

}
