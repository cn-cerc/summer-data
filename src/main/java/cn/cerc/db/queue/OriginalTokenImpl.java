package cn.cerc.db.queue;

/**
 * 跨集群主机token支持
 * 
 * @author 张弓
 *
 */
public interface OriginalTokenImpl {

    /**
     * 
     * @return 企业帐套原始产业别
     */
    String getOriginal();

    /**
     * 
     * @return 企业帐套访问令牌
     */
    String getToken();

}
