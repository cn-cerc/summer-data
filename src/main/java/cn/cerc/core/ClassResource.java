package cn.cerc.core;

import java.util.HashMap;

public class ClassResource {
    private static HashMap<String, LanguageResource> buffer = new HashMap<>();
    private LanguageResource stringResource;
    private String classPath;

    public ClassResource(String resourceFile, Class<?> clazz) {
        stringResource = buffer.get(resourceFile);
        if (stringResource == null) {
            stringResource = new LanguageResource(resourceFile);
            buffer.put(resourceFile, stringResource);
        }
        this.classPath = clazz.getName();
    }

    public String getString(int id, String string) {
        return stringResource.getString(String.format("%s.%d", this.classPath, id), string);
    }

    public String getFile(String fileName) {
        return stringResource.getString(String.format("%s.file.%s", this.classPath, fileName), fileName);
    }
}
