package cn.cerc.db.core;

public interface IConfig {
    String getProperty(String key, String def);

    /**
     * 从 properties 读取配置项
     * 
     * @return 返回配置，可为null
     */
    default String getProperty(String key) {
        return this.getProperty(key, null);
    }
}
