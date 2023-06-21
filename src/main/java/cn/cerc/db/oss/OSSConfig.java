package cn.cerc.db.oss;

public abstract class OSSConfig {
    /**
     * 设置OBSClient使用的最大连接数，默认1024
     */
    protected static final int maxConnections = 1024;
    /**
     * 设置请求超时时间，默认3秒
     */
    protected static final int socketTimeout = 3 * 1000;
    /**
     * 设置失败请求重试次数，默认3次
     */
    protected static final int maxErrorRetry = 3;

    /**
     * 终端地址
     */
    protected String oss_endpoint;

    /**
     * 桶名称
     */
    protected String oss_bucket;
    /**
     * 对外访问地址
     */
    protected String oss_site;
    /**
     * AK
     */
    protected String oss_accessKeyId;
    /**
     * SK
     */
    protected String oss_accessKeySecret;
}
