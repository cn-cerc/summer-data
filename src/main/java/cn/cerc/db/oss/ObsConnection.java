package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.springframework.stereotype.Component;

import com.aliyun.oss.OSSException;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.ObsObject;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.IConnection;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

@Component
public class ObsConnection implements IConnection {

    /**
     * 阿里云/华为云
     */
    private OSSTypeEnum ossType;

    public static String getOssClient() {
        return oss_client;
    }

    public OSSTypeEnum getOssType() {
        return ossType;
    }

    public void setOssType(OSSTypeEnum ossType) {
        this.ossType = ossType;
    }

    private static final String oss_client = "oss.client"; // 配置选择华为的OBS或者阿里的OSS
    private static String bucket;
    private static String site;

    // IHandle 标识
    public static final String sessionId = "ossSession";
    private static final IConfig config = ServerConfig.getInstance();
    private static volatile IOssAction client;

    @Override
    public OSSConfig getClient() {
        this.setOssType(ServerConfig.getOSSTypeConfig());

        if (client == null) {
            synchronized (ObsConnection.class) {
                if (client == null) {
                    OSSConfig ossConfig = null;
                    
                    switch (key) {
                    case value: {
                        
                        yield type;
                    }
                    default:
                        throw new IllegalArgumentException("Unexpected value: " + key);
                    }
                    if (OSSTypeEnum.Aliyun_OSS == getOssType()) {
                        ossConfig = new AliyunOSSConfig();
                        client = new AliyunOBSClient(ossConfig);
                    } else if (OSSTypeEnum.Huawei_OBS == getOssType()) {
                        ossConfig = new HuaweiOBSConfig();
                        client = new AliyunOBSClient(ossConfig);
                    }

                }
            }
        }
        return client;
    }

    // 获取指定的数据库是否存在
    public boolean exist(String bucket) {
        return getClient().headBucket(bucket);
    }

    // 获取所有的列表
    public List<ObsBucket> getBuckets() {
        return getClient().listBuckets(new ListBucketsRequest());
    }

    // 上传文件
    public void upload(String fileName, File file) {
        upload(getBucket(), fileName, file);
    }

    // 上传文件流
    public void upload(String fileName, InputStream inputStream) {
        upload(getBucket(), fileName, inputStream);
    }

    public void upload(String bucket, String fileName, InputStream inputStream) {
        getClient().putObject(bucket, fileName, inputStream);
    }

    public void upload(String bucket, String fileName, File file) {
        getClient().putObject(bucket, fileName, file);
    }

    /**
     * @param fileName    原文件
     * @param newFileName 目标文件
     */
    public void copy(String fileName, String newFileName) {
        copy(getBucket(), fileName, getBucket(), newFileName);
    }

    /**
     * @param bucket      原bucket
     * @param newBucket   目标bucket
     * @param fileName    原文件
     * @param newFileName 目标文件
     */
    public void copy(String bucket, String fileName, String newBucket, String newFileName) {
        getClient().copyObject(bucket, fileName, newBucket, newFileName);
    }

    // 下载文件到localFile
    public boolean download(String fileName, String localFile) {
        if (Utils.isEmpty(fileName))
            return false;
        if (Utils.isEmpty(localFile))
            return false;

        ObsObject obsObject = getClient().getObject(bucket, localFile);
        try (InputStream input = obsObject.getObjectContent()) {
            Path path = Path.of(localFile);
            Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        try {
            long fileSize = Files.size(Path.of(localFile));
            return fileSize > 0 && obsObject.getMetadata().getContentLength() == fileSize;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 下载文件
    public InputStream download(String fileName) {
        return getClient().getObject(bucket, fileName).getObjectContent();
    }

    // 删除文件
    public void delete(String fileName) {
        delete(getBucket(), fileName);
    }

    // 指定Bucket删除文件
    public void delete(String bucket, String fileName) {
        getClient().deleteObject(bucket, fileName);
    }

    public String getContent(String fileName) {
        try {
            StringBuffer sb = new StringBuffer();
            ObsObject obj = getClient().getObject(getBucket(), fileName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
            while (true) {
                String line;
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                sb.append(line);
            }
            return sb.toString();
        } catch (OSSException | IOException e) {
            return null;
        }
    }

    /**
     * 判断指定的文件名是否存在
     *
     * @param fileName 带完整路径的文件名
     * @return 若存在则返回true
     */
    public boolean existsFile(String fileName) {
        if (Utils.isEmpty(fileName))
            return false;
        return getClient().doesObjectExist(getBucket(), fileName);
    }

    @Deprecated
    public String getFileUrl(String fileName, String def) {
        return this.buildFileUrl(fileName, def);
    }

    /**
     * 构建OSS文件的绝对访问路径
     *
     * @param fileName 带完整路径的文件名
     * @param def      默认值
     * @return 若存在则返回路径，否则返回默认值
     */
    public String buildFileUrl(String fileName, String def) {
        if (existsFile(fileName)) {
            return String.format("%s/%s", this.getSite(), fileName);
        } else {
            return def;
        }
    }

    public String getBucket() {
        if (Utils.isEmpty(bucket))
            bucket = config.getProperty(ObsConnection.oss_bucket);
        return bucket;
    }

    public String getSite() {
        if (Utils.isEmpty(site))
            site = config.getProperty(ObsConnection.oss_site);
        return site;
    }

}
