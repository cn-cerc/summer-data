package cn.cerc.db.core;

public interface IConfig {
    String getProperty(String key, String def);

    /**
     * 从 properties 读取配置项
     * 
     * @param key
     * @return 返回配置，可为null
     */
    default String getProperty(String key) {
        return this.getProperty(key, null);
    }

    default Variant bind(String key, Object def) {
        var result = this.getProperty(key);
        return new Variant(result != null ? result : def).setKey(key);
    }

    default Variant bind(String key) {
        return this.bind(key, null);
    }
}
