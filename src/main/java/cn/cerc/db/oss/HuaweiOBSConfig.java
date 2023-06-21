package cn.cerc.db.oss;

public class HuaweiOBSConfig extends OSSConfig {

    public HuaweiOBSConfig() {
        this.oss_endpoint = "obs.endpoint";
        this.oss_bucket = "obs.bucket";
        this.oss_site = "obs.site";
        this.oss_accessKeyId = "obs.accessKeyId";
        this.oss_accessKeySecret = "obs.accessKeySecret";
    }
}
