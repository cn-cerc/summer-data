package cn.cerc.core;

import java.util.HashMap;

public class ClassResource {
    private static HashMap<String, LanguageResource> buffer = new HashMap<>();
    private LanguageResource stringResource;
    private String clasPath;

    public ClassResource(String resourceFile, Class<?> clazz) {
        LanguageResource stringResource = buffer.get(resourceFile);
        if (stringResource == null) {
            stringResource = new LanguageResource(resourceFile);
            buffer.put(resourceFile, stringResource);
        }
        this.clasPath = clazz.getClass().getName();
    }

    public String getString(int id, String string) {
        return stringResource.getString(String.format("%s.%d", this.clasPath, id), string);
    }

    public String getFile(String fileName) {
        return stringResource.getString(String.format("%s.file.%s", this.clasPath, fileName), fileName);
    }
}
