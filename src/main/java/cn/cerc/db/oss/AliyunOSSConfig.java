package cn.cerc.db.oss;

import org.springframework.stereotype.Component;

import com.obs.services.ObsConfiguration;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

@Component
public class AliyunOSSConfig extends OSSConfig {

    private static final IConfig config = ServerConfig.getInstance();

    ObsConfiguration oss_config;

    public AliyunOSSConfig() {
        String endpoint = "oss.endpoint";
        String bucket = "oss.bucket";
        String site = "oss.site";
        String accessKeyId = "oss.accessKeyId";
        String accessKeySecret = "oss.accessKeySecret";

        this.oss_endpoint = config.getProperty(endpoint);
        if (Utils.isEmpty(this.oss_endpoint))
            throw new RuntimeException(String.format("the property %s is empty", endpoint));

        this.oss_accessKeyId = config.getProperty(accessKeyId);
        if (Utils.isEmpty(this.oss_accessKeyId))
            throw new RuntimeException(String.format("the property %s is empty", accessKeyId));

        this.oss_accessKeySecret = config.getProperty(accessKeySecret);
        if (Utils.isEmpty(this.oss_accessKeySecret))
            throw new RuntimeException(String.format("the property %s is empty", accessKeySecret));

        this.oss_bucket = config.getProperty(bucket);
        if (Utils.isEmpty(this.oss_bucket))
            throw new RuntimeException(String.format("the property %s is empty", bucket));

        this.oss_site = config.getProperty(site);
        if (Utils.isEmpty(this.oss_site))
            throw new RuntimeException(String.format("the property %s is empty", site));

        this.oss_config = setConfig(OSSConfig.maxConnections, OSSConfig.socketTimeout, OSSConfig.maxErrorRetry);

    }

    private ObsConfiguration setConfig(int maxConn, int timeout, int reTryTimes) {
        oss_config.setMaxConnections(maxConn);
        oss_config.setSocketTimeout(timeout);
        oss_config.setMaxErrorRetry(reTryTimes);
        return oss_config;
    }

}
