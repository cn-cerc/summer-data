package cn.cerc.db.oss;

import java.io.File;
import java.io.InputStream;
import java.util.List;

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
}
