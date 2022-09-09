package cn.cerc.db.core;

public interface IConfig {
    String getProperty(String key, String def);

    default String getProperty(String key) {
        return this.getProperty(key, null);
    }
}
