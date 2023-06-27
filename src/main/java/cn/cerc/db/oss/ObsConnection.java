package cn.cerc.db.oss;

import org.springframework.stereotype.Component;

import cn.cerc.db.core.IConnection;
import cn.cerc.db.core.ServerConfig;

@Component
public class ObsConnection implements IConnection {

    private OSSTypeEnum ossType;

    public OSSTypeEnum getOssType() {
        return ossType;
    }

    public void setOssType(OSSTypeEnum ossType) {
        this.ossType = ossType;
    }

    // IHandle 标识
    public static final String sessionId = "ossSession";
    private static volatile IOssAction client;

    /**
     * new ObsConnection().getClient.download();
     */
    @Override
    public IOssAction getClient() {
        this.setOssType(ServerConfig.getOSSTypeConfig());

        if (client == null) {
            synchronized (ObsConnection.class) {
                if (client == null) {
                    switch (getOssType()) {
                    case Aliyun_OSS:
                        client = new AliyunOBSClient();
                        break;
                    case Huawei_OBS:
                        client = new HuaweiOBSClient();
                        break;
                    default:
                        break;
                    }
                }
            }
        }
        return client;
    }
}