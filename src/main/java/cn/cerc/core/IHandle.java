package cn.cerc.core;

public interface IHandle extends AutoCloseable {

    // 帐套代码（公司别）
    public String getCorpNo();

    // 用户帐号
    public String getUserCode();

    // 自定义参数，注：若key=null则返回实现接口的对象本身
    public Object getProperty(String key);

    // 用户姓名
    public String getUserName();

    // 设置自定义参数
    public void setProperty(String key, Object value);

    // 直接设置成登录成功状态，用于定时服务时初始化等，会生成内存临时的token
    public boolean init(String bookNo, String userCode, String clientCode);

    // 在登录成功并生成token后，传递token值进行初始化
    public boolean init(String token);

    // 返回当前是否为已登入状态
    public boolean logon();

    // // 关闭资源
    // default public void closeConnections() {
    // }
    //
    // // 用户姓名
    // default public String getUserName() {
    // return getUserCode();
    // }
    //
    // // 设置自定义参数
    // default public void setProperty(String key, Object value) {
    // throw new RuntimeException("调用了未被实现的接口");
    // }
    //
    // // 直接设置成登录成功状态，用于定时服务时初始化等，会生成内存临时的token
    // default public boolean init(String bookNo, String userCode, String
    // clientCode) {
    // throw new RuntimeException("调用了未被实现的接口");
    // }
    //
    // // 在登录成功并生成token后，传递token值进行初始化
    // default public boolean init(String token) {
    // throw new RuntimeException("调用了未被实现的接口");
    // }
    //
    // // 返回当前是否为已登入状态
    // default boolean logon() {
    // return false;
    // }

}
