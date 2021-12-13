package cn.cerc.db.oss;

import java.io.File;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.ObjectMetadata;

import cn.cerc.core.ClassResource;
import cn.cerc.db.SummerDB;
import cn.cerc.db.core.IHandle;

/**
 * 统一到一个工具类
 */
@Deprecated
public class OssDisk {
    private static final ClassResource res = new ClassResource(OssDisk.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(OssDisk.class);

    private String localPath;

    public OssDisk(IHandle handle) {
    }

    // 默认Bucket上传文件流
    public void upload(String fileName, InputStream inputStream) {
        AliyunStorage.upload(fileName, inputStream);
    }

    // 指定Bucket上传文件流
    public void upload(String bucket, String fileName, InputStream inputStream) {
        AliyunStorage.upload(bucket, fileName, inputStream);
    }

    // 上传文件
    public boolean upload(String remoteFile, String localFile) {
        // 上传本地文件到服务器
        // 例：upload("D:\\oss\\temp.png", "131001/Default/131001/temp.png")
        File file = new File(localFile);
        if (!file.exists()) {
            throw new RuntimeException(String.format(res.getString(1, "文件不存在：%s"), localFile));
        }
        try {
            ObjectMetadata summary = AliyunStorage.getClient().getObjectMetadata(AliyunStorage.getBucket(),
                    remoteFile);
            if (summary != null && summary.getContentLength() == file.length()) {
                log.info("ignore upload, because the local file has the same size as cloud file");
                return true;
            }
        } catch (OSSException e) {
            log.info("there is no such file on the server, start uploading ...");
        }

        AliyunStorage.getClient().putObject(AliyunStorage.getBucket(), remoteFile, file);
        ObjectMetadata metadata = AliyunStorage.getClient().getObjectMetadata(AliyunStorage.getBucket(),
                remoteFile);
        return file.exists() && metadata.getContentLength() == file.length();
    }

    // 下载文件
    public boolean download(String fileName) {
        if (localPath == null || "".equals(localPath)) {
            throw new RuntimeException(res.getString(2, "localPath 必须先进行设置！"));
        }

        // 创建本地目录
        String localFile = localPath + fileName.replace('/', '\\');
        createFolder(localFile);

        return AliyunStorage.download(fileName, localFile);
    }

    // 默认Bucket删除文件
    public void delete(String fileName) {
        AliyunStorage.delete(fileName);
    }

    // 指定Bucket删除文件
    public void delete(String bucket, String fileName) {
        AliyunStorage.delete(bucket, fileName);
    }

    // 拷贝Object
    public void copyObject(String srcBucketName, String srcKey, String destBucketName, String destKey) {
        /*
         * sample: srcBucketName = "scmfiles" srcKey = "Products\010001\钻石.jpg";
         * destBucketName = "vinefiles"; destKey = "131001\product\0100001\钻石.jpg";
         */
        CopyObjectResult result = AliyunStorage.getClient().copyObject(srcBucketName, srcKey, destBucketName,
                destKey);

        // 打印结果
        log.info("ETag: {}, LastModified: {}", result.getETag(), result.getLastModified());
    }

    public OSS getClient() {
        return AliyunStorage.getClient();
    }

    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    // 如果文件所在的文件目录不存在，则创建之
    private void createFolder(String fileName) {
        String tmpPath = fileName.substring(0, fileName.lastIndexOf("\\") + 1);
        String subPath = tmpPath;
        int fromIndex = 0;
        while (tmpPath.indexOf("\\", fromIndex) > -1) {
            int beginIndex = tmpPath.indexOf("\\", fromIndex);
            if (fromIndex == -1) {
                break;
            }
            subPath = tmpPath.substring(0, beginIndex);
            fromIndex = subPath.length() + 1;
            if (subPath.length() > 2) {
                File file = new File(subPath);
                // 如果文件夹不存在则创建
                if (!file.exists() && !file.isDirectory()) {
                    file.mkdir();
                }
            }
        }
    }
}
