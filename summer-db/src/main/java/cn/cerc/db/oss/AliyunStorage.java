package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

import cn.cerc.core.IConfig;
import cn.cerc.core.Utils;
import cn.cerc.db.core.ServerConfig;

/**
 * 阿里云对象存储操作工具
 */
public class AliyunStorage {

    private static final IConfig config = ServerConfig.getInstance();

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

    private static volatile OSS client;

    public static OSS getClient() {
        if (client == null) {
            synchronized (AliyunStorage.class) {
                if (client == null) {
                    // 如果连接被意外断开了,那么重新建立连接
                    String endpoint = config.getProperty(AliyunStorage.oss_endpoint);
                    if (Utils.isEmpty(endpoint))
                        throw new RuntimeException(
                                String.format("the property %s is empty", AliyunStorage.oss_endpoint));

                    String accessKeyId = config.getProperty(AliyunStorage.oss_accessKeyId);
                    if (Utils.isEmpty(accessKeyId))
                        throw new RuntimeException(
                                String.format("the property %s is empty", AliyunStorage.oss_accessKeyId));

                    String accessKeySecret = config.getProperty(AliyunStorage.oss_accessKeySecret);
                    if (Utils.isEmpty(accessKeySecret))
                        throw new RuntimeException(
                                String.format("the property %s is empty", AliyunStorage.oss_accessKeySecret));

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

    public static String getBucket() {
        String bucket = config.getProperty(AliyunStorage.oss_bucket);
        if (Utils.isEmpty(bucket))
            throw new RuntimeException(String.format("the property %s is empty", AliyunStorage.oss_bucket));
        return bucket;
    }

    public static String getSite() {
        String site = config.getProperty(AliyunStorage.oss_site);
        if (Utils.isEmpty(site))
            throw new RuntimeException(String.format("the property %s is empty", AliyunStorage.oss_site));
        return site;
    }

    public static OSSObject getObject(String ossFile) {
        return AliyunStorage.getClient().getObject(AliyunStorage.getBucket(), ossFile);
    }

    // 获取指定的数据库是否存在
    public boolean exist(String bucket) {
        return AliyunStorage.getClient().doesBucketExist(bucket);
    }

    // 获取所有的列表
    public List<Bucket> getBuckets() {
        return AliyunStorage.getClient().listBuckets();
    }

    // 上传文件
    public static void upload(String fileName, InputStream inputStream) {
        upload(AliyunStorage.getBucket(), fileName, inputStream);
    }

    // 指定上传Bucket
    public static void upload(String bucket, String fileName, InputStream inputStream) {
        // 例：upload(inputStream, "131001/Default/131001/temp.txt")
        AliyunStorage.getClient().putObject(bucket, fileName, inputStream);
    }

    // 下载文件
    public static boolean download(String fileName, String localFile) {
        GetObjectRequest param = new GetObjectRequest(AliyunStorage.getBucket(), fileName);
        File file = new File(localFile);
        ObjectMetadata metadata = AliyunStorage.getClient().getObject(param, file);
        return file.exists() && metadata.getContentLength() == file.length();
    }

    // 删除文件
    public static void delete(String fileName) {
        delete(AliyunStorage.getBucket(), fileName);
    }

    // 指定Bucket删除文件
    public static void delete(String bucket, String fileName) {
        AliyunStorage.getClient().deleteObject(bucket, fileName);
    }

    public static String getContent(String fileName) {
        try {
            StringBuffer sb = new StringBuffer();
            // ObjectMetadata meta = client.getObjectMetadata(this.getBucket(),
            // fileName);
            // if (meta.getContentLength() == 0)
            // return null;
            OSSObject obj = AliyunStorage.getObject(fileName);
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
    public static boolean existsFile(String fileName) {
        try {
            OSSObject obj = AliyunStorage.getObject(fileName);
            return obj.getObjectMetadata().getContentLength() > 0;
        } catch (OSSException e) {
            return false;
        }
    }

    /**
     * 返回可用的文件名称
     *
     * @param fileName 带完整路径的文件名
     * @param def      默认值
     * @return 若存在则返回路径，否则返回默认值
     */
    public static String getFileUrl(String fileName, String def) {
        if (existsFile(fileName)) {
            return String.format("%s/%s", AliyunStorage.getSite(), fileName);
        } else {
            return def;
        }
    }

    /**
     * 拷贝Object
     * 
     * sample: fromBucket = "scmfiles" fromKey = "Products\010001\钻石.jpg";
     * 
     * toBucket = "vinefiles"; toKey = "131001\product\0100001\钻石.jpg";
     */
    public static void copyObject(String fromBucket, String fromKey, String toBucket, String toKey) {
        AliyunStorage.getClient().copyObject(fromBucket, fromKey, toBucket, toKey);
    }
}
