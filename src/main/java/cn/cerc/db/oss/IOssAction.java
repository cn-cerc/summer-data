package cn.cerc.db.oss;

import java.io.InputStream;
import java.util.List;

public interface IOssAction {

    boolean exist(String bucket);

    List<String> getBuckets();

    String getBucket();

    void upload(String bucket, String fileName, InputStream is);

    void copy(String bucket, String fileName, String newBucket, String newFileName);

    InputStream download(String bucket, String fileName);

    void delete(String bucket, String fileName);

    boolean existsFile(String bucket, String fileName);

}
