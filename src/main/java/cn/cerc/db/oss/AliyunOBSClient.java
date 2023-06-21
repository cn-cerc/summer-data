package cn.cerc.db.oss;

import java.io.InputStream;
import java.util.List;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;

public class AliyunOBSClient extends ObsClient implements IOssAction {

    public AliyunOBSClient(String accessKey, String secretKey, String securityToken, ObsConfiguration config) {
        super(accessKey, secretKey, securityToken, config);
    }

    @Override
    public boolean exist(String bucket) {
        return false;
    }

    @Override
    public List<String> getBuckets() {
        return null;
    }

    @Override
    public String getBucket() {
        return null;
    }

    @Override
    public void upload(String bucket, String fileName, InputStream is) {

    }

    @Override
    public void copy(String bucket, String fileName, String newBucket, String newFileName) {

    }

    @Override
    public InputStream download(String bucket, String fileName) {
        return null;
    }

    @Override
    public void delete(String bucket, String fileName) {

    }

    @Override
    public boolean existsFile(String bucket, String fileName) {
        return false;
    }

}
