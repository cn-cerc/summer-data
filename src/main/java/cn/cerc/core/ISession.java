package cn.cerc.core;

public interface ISession extends AutoCloseable{
    
    // 自定义参数，注：若key=null则返回实现接口的对象本身
    Object getProperty(String key);
    
    // 设置自定义参数
    void setProperty(String key, Object value);

    // 帐套代码（公司别）
    String getCorpNo();

    // 用户帐号
    String getUserCode();

    // 用户姓名
    String getUserName();

    // 返回当前是否为已登入状态
    boolean logon();

    void close();
}
