package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.springframework.stereotype.Component;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.IConnection;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

@Component
public class OssConnection implements IConnection {

    // 设置连接地址
    public static final String oss_endpoint = "oss.endpoint";
    // 连接区域
    public static final String oss_bucket = "oss.bucket";
    // 对外访问地址
    public static final String oss_site = "oss.site";
    // 连接id
    public static final String oss_accessKeyId = "oss.accessKeyId";
    // 连接密码
    public static final String oss_accessKeySecret = "oss.accessKeySecret";

    private static String bucket;
    private static String site;

    // IHandle 标识
    public static final String sessionId = "ossSession";
    private static final IConfig config = ServerConfig.getInstance();
    private static volatile OSS client;

    @Override
    public OSS getClient() {
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

                    ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
                    // 设置OSSClient使用的最大连接数，默认1024
                    conf.setMaxConnections(1024);
                    // 设置请求超时时间，默认3秒
                    conf.setSocketTimeout(3 * 1000);
                    // 设置失败请求重试次数，默认3次
                    conf.setMaxErrorRetry(3);

                    // 创建OSSClient实例
                    client = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret, conf);
                }
            }
        }
        return client;
    }

    // 获取指定的数据库是否存在
    public boolean exist(String bucket) {
        return getClient().doesBucketExist(bucket);
    }

    // 获取所有的列表
    public List<Bucket> getBuckets() {
        return getClient().listBuckets();
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
        GetObjectRequest param = new GetObjectRequest(getBucket(), fileName);
        File file = new File(localFile);
        ObjectMetadata metadata = getClient().getObject(param, file);
        return file.exists() && metadata.getContentLength() == file.length();
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
            // ObjectMetadata meta = client.getObjectMetadata(this.getBucket(),
            // fileName);
            // if (meta.getContentLength() == 0)
            // return null;
            OSSObject obj = getClient().getObject(getBucket(), fileName);
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
        try {
            OSSObject obj = getClient().getObject(getBucket(), fileName);
            return obj.getObjectMetadata().getContentLength() > 0;
        } catch (OSSException e) {
            return false;
        }
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
