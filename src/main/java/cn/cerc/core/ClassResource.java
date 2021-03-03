package cn.cerc.core;

import java.util.HashMap;
import java.util.Map;

public class ClassResource {
    private static Map<String, LanguageResource> buffer = new HashMap<>();
    private LanguageResource stringResource;
    private String languageId;
    private String classPath;
    private String resourceFile;

    public ClassResource(String resourceFile, Class<?> clazz) {
        this.classPath = clazz.getName();
        this.resourceFile = resourceFile;
        initResource();
    }

    public ClassResource(String resourceFile, Object owner) {
        if (owner instanceof IUserLanguage) {
            this.languageId = ((IUserLanguage) owner).getLanguageId();
        }
        this.classPath = owner.getClass().getName();
        this.resourceFile = resourceFile;
        initResource();
    }

    private void initResource() {
        stringResource = buffer.get(resourceFile);
        if (stringResource == null) {
            stringResource = new LanguageResource(resourceFile, this.languageId);
            buffer.put(resourceFile, stringResource);
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
