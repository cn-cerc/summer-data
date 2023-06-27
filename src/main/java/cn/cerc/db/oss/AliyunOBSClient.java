package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Utils;

public class AliyunOBSClient implements IOssAction {

    private AliyunOSSConfig config;
    private OSS ossClient;

    public AliyunOBSClient() {
        config = new AliyunOSSConfig();
        this.setOssClient(new OSSClientBuilder().build(config.oss_endpoint, config.oss_accessKeyId,
                config.oss_accessKeySecret, config.oss_config));
    }

    public OSS getOssClient() {
        return ossClient;
    }

    public void setOssClient(OSS ossClient) {
        this.ossClient = ossClient;
    }

    // 获取指定的数据库是否存在
    @Override
    public boolean exist(String bucket) {
        return getOssClient().doesBucketExist(bucket);
    }

    // 获取所有的列表
    @Override
    public List<String> getBuckets() {
        return getOssClient().listBuckets().stream().map(item -> item.getName()).toList();
    }

    /**
     * 上传文件
     */
    @Override
    public void upload(String fileName, File file) {
        upload(getBucket(), fileName, file);
    }

    /**
     * 上传文件流
     */
    @Override
    public void upload(String fileName, InputStream inputStream) {
        upload(getBucket(), fileName, inputStream);
    }

    /**
     * 上传文件流到指定Bucket
     */
    @Override
    public void upload(String bucket, String fileName, InputStream inputStream) {
        getOssClient().putObject(bucket, fileName, inputStream);
    }

    /**
     * 上传文件到指定Bucket
     */
    @Override
    public void upload(String bucket, String fileName, File file) {
        getOssClient().putObject(bucket, fileName, file);
    }

    /**
     * @param fileName    原文件
     * @param newFileName 目标文件
     */
    @Override
    public void copy(String fileName, String newFileName) {
        copy(getBucket(), fileName, getBucket(), newFileName);
    }

    /**
     * @param bucket      原bucket
     * @param newBucket   目标bucket
     * @param fileName    原文件
     * @param newFileName 目标文件
     */
    @Override
    public void copy(String bucket, String fileName, String newBucket, String newFileName) {
        getOssClient().copyObject(bucket, fileName, newBucket, newFileName);
    }

    // 下载文件
    @Override
    public boolean download(String fileName, String localFile) {
        if (Utils.isEmpty(fileName))
            return false;
        if (Utils.isEmpty(localFile))
            return false;
        GetObjectRequest param = new GetObjectRequest(getBucket(), fileName);
        File file = new File(localFile);
        ObjectMetadata metadata = getOssClient().getObject(param, file);
        return file.exists() && metadata.getContentLength() == file.length();
    }

    // 下载文件
    @Override
    public InputStream download(String fileName) {
        GetObjectRequest param = new GetObjectRequest(getBucket(), fileName);
        return getOssClient().getObject(param).getObjectContent();
    }

    // 删除文件
    @Override
    public void delete(String fileName) {
        delete(getBucket(), fileName);
    }

    // 指定Bucket删除文件
    @Override
    public void delete(String bucket, String fileName) {
        getOssClient().deleteObject(bucket, fileName);
    }

    @Override
    public String getContent(String fileName) {
        try {
            StringBuffer sb = new StringBuffer();
            OSSObject obj = getOssClient().getObject(getBucket(), fileName);
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
    @Override
    public boolean existsFile(String fileName) {
        if (Utils.isEmpty(fileName))
            return false;
        try {
            OSSObject obj = getOssClient().getObject(getBucket(), fileName);
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
    @Override
    public String buildFileUrl(String fileName, String def) {
        if (existsFile(fileName)) {
            return String.format("%s/%s", this.getSite(), fileName);
        } else {
            return def;
        }
    }

    @Override
    public String getBucket() {
        return config.oss_bucket;
    }

    @Override
    public String getSite() {
        return config.oss_site;
    }

    /**
     * @return 阿里的 com.aliyun.oss.model.ObjectMetadata;
     */
    @Override
    public ObjectMetadata getObjectMetadata(String bucket, String remoteFile) {
        return getOssClient().getObjectMetadata(bucket, remoteFile);
    }

    /**
     * @param expireTime 过期时间
     * @param ImageParam 转码参数
     */
    @Override
    public URL generatePresignedUrl(String bucket, String fileName, Datetime expireTime, String ImageParam) {
        GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucket, fileName);
        req.setExpiration(expireTime.asBaseDate());
        req.setProcess(ImageParam);
        return getOssClient().generatePresignedUrl(req);
    }

    @Override
    public List<String> listFiles(String bucket) {
        return getOssClient().listObjects(bucket).getObjectSummaries().stream().map(item -> item.getKey()).toList();
    }

}