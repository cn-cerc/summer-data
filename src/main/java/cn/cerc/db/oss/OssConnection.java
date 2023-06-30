package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.ObsObject;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.IConnection;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

@Component
public class OssConnection implements IConnection {
    private static final Logger log = LoggerFactory.getLogger(OssConnection.class);

    // 设置连接地址
    public static final String oss_endpoint = "huawei.oss.endpoint";
    // 连接区域
    public static final String oss_bucket = "huawei.oss.bucket";
    // 对外访问地址
    public static final String oss_site = "huawei.oss.site";
    // 连接id
    public static final String oss_accessKeyId = "huawei.oss.accessKeyId";
    // 连接密码
    public static final String oss_accessKeySecret = "huawei.oss.accessKeySecret";

    private static String bucket;
    private static String site;

    // IHandle 标识
    public static final String sessionId = "ossSession";
    private static final IConfig config = ServerConfig.getInstance();
    private static volatile ObsClient client;

    @Override
    public ObsClient getClient() {
        if (client == null) {
            synchronized (OssConnection.class) {
                if (client == null) {
                    // 如果连接被意外断开了,那么重新建立连接
                    String endpoint = config.getProperty(OssConnection.oss_endpoint);
                    if (Utils.isEmpty(endpoint))
                        throw new RuntimeException(
                                String.format("the property %s is empty", OssConnection.oss_endpoint));

                    String accessKeyId = config.getProperty(OssConnection.oss_accessKeyId);
                    if (Utils.isEmpty(accessKeyId))
                        throw new RuntimeException(
                                String.format("the property %s is empty", OssConnection.oss_accessKeyId));

                    String accessKeySecret = config.getProperty(OssConnection.oss_accessKeySecret);
                    if (Utils.isEmpty(accessKeySecret))
                        throw new RuntimeException(
                                String.format("the property %s is empty", OssConnection.oss_accessKeySecret));

                    ObsConfiguration conf = new ObsConfiguration();
                    // 设置OSSClient使用的最大连接数，默认1024
                    conf.setMaxConnections(1024);
                    // 设置请求超时时间，默认3秒
                    conf.setSocketTimeout(3 * 1000);
                    // 设置失败请求重试次数，默认3次
                    conf.setMaxErrorRetry(3);
                    conf.setEndPoint(endpoint);

                    client = new ObsClient(accessKeyId, accessKeySecret, conf);
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
    public void upload(String fileName, InputStream inputStream) {
        upload(getBucket(), fileName, inputStream);
    }

    // 指定上传Bucket
    public void upload(String bucket, String fileName, InputStream inputStream) {
        // 例：upload(inputStream, "131001/Default/131001/temp.txt")
        getClient().putObject(bucket, fileName, inputStream);
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

    // 下载文件
    public boolean download(String fileName, String localFile) {
        if (Utils.isEmpty(fileName))
            return false;
        if (Utils.isEmpty(localFile))
            return false;

        ObsObject obsObject = getClient().getObject(getBucket(), localFile);
        try (InputStream input = obsObject.getObjectContent()) {
            Path path = Path.of(localFile);
            Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return false;
        }

        try {
            long fileSize = Files.size(Path.of(localFile));
            return fileSize > 0 && obsObject.getMetadata().getContentLength() == fileSize;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    // 下载文件
    public InputStream download(String fileName) {
        GetObjectRequest param = new GetObjectRequest(getBucket(), fileName);
        return getClient().getObject(param).getObjectContent();
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
        } catch (IOException e) {
            log.error(e.getMessage(), e);
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
            bucket = config.getProperty(OssConnection.oss_bucket);
        return bucket;
    }

    public String getSite() {
        if (Utils.isEmpty(site))
            site = config.getProperty(OssConnection.oss_site);
        return site;
    }

}
