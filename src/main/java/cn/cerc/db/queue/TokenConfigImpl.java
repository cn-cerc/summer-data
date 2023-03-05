package cn.cerc.db.queue;

import java.util.Optional;

import cn.cerc.db.core.IHandle;

/**
 * 跨集群主机token支持
 * 
 * @author 张弓
 *
 */
public interface TokenConfigImpl extends IHandle {

    /**
     * 
     * @return 企业帐套原始产业别
     */
    Optional<String> getOriginal();

    /**
     * 
     * @return 企业帐套访问令牌
     */
    Optional<String> getToken();

}
