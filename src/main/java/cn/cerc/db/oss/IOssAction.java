package cn.cerc.db.oss;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import cn.cerc.db.core.Datetime;

public interface IOssAction {

    boolean exist(String bucket);

    List<String> getBuckets();

    void upload(String fileName, File file);

    void upload(String fileName, InputStream inputStream);

    void upload(String bucket, String fileName, InputStream inputStream);

    void upload(String bucket, String fileName, File file);

    void copy(String fileName, String newFileName);

    void copy(String bucket, String fileName, String newBucket, String newFileName);

    boolean download(String fileName, String localFile);

    InputStream download(String fileName);

    void delete(String fileName);

    void delete(String bucket, String fileName);

    String getContent(String fileName);

    boolean existsFile(String fileName);

    String buildFileUrl(String fileName, String def);

    String getBucket();

    String getSite();

    /**
     * 获取文件元数据
     * 
     * @param bucket     桶名
     * @param remoteFile 文件名
     * @return 阿里客户端返回 com.aliyun.oss.model.ObjectMetadata; <br>
     *         华为客户端返回 com.obs.services.model.ObjectMetadata;
     */
    Object getObjectMetadata(String bucket, String remoteFile);

    URL generatePresignedUrl(String bucket, String fileName, Datetime expireTime, String ImageParam);

    List<String> listFiles(String bucket);
}
