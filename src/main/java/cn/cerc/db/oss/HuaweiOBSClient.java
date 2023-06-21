package cn.cerc.db.oss;

import java.io.InputStream;
import java.util.List;

public class HuaweiOBSClient implements IOssAction {
    
    huaweiClient;
    
    

    @Override
    public boolean exist(String bucket) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<String> getBuckets() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getBucket() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void upload(String bucket, String fileName, InputStream is) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void copy(String bucket, String fileName, String newBucket, String newFileName) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public InputStream download(String bucket, String fileName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(String bucket, String fileName) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean existsFile(String bucket, String fileName) {
        // TODO Auto-generated method stub
        return false;
    }

}
