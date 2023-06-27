package cn.cerc.db.oss;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.oss.OSSException;
import com.obs.services.ObsClient;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.TemporarySignatureRequest;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Datetime.DateType;
import cn.cerc.db.core.Utils;

public class HuaweiOBSClient implements IOssAction {

    private HuaweiOBSConfig config;
    private ObsClient ossClient;

    public HuaweiOBSClient() {
        config = new HuaweiOBSConfig();
        this.setOssClient(new ObsClient(config.oss_accessKeyId, config.oss_accessKeySecret, config.oss_endpoint));
    }

    public ObsClient getOssClient() {
        return ossClient;
    }

    public void setOssClient(ObsClient ossClient) {
        this.ossClient = ossClient;
    }

    // 获取指定的数据库是否存在
    @Override
    public boolean exist(String bucket) {
        return getOssClient().headBucket(bucket);
    }

    // 获取所有的列表
    @Override
    public List<String> getBuckets() {
        return getOssClient().listBuckets(new ListBucketsRequest()).stream().map(item -> item.getBucketName()).toList();
    }

    // 上传文件
    @Override
    public void upload(String fileName, File file) {
        upload(getBucket(), fileName, file);
    }

    // 上传文件流
    @Override
    public void upload(String fileName, InputStream inputStream) {
        upload(getBucket(), fileName, inputStream);
    }

    @Override
    public void upload(String bucket, String fileName, InputStream inputStream) {
        getOssClient().putObject(bucket, fileName, inputStream);
    }

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

    // 下载文件到localFile
    @Override
    public boolean download(String fileName, String localFile) {
        if (Utils.isEmpty(fileName))
            return false;
        if (Utils.isEmpty(localFile))
            return false;

        ObsObject obsObject = getOssClient().getObject(config.oss_bucket, localFile);
        try (InputStream input = obsObject.getObjectContent()) {
            Path path = Path.of(localFile);
            Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        try {
            long fileSize = Files.size(Path.of(localFile));
            return fileSize > 0 && obsObject.getMetadata().getContentLength() == fileSize;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 下载文件
    @Override
    public InputStream download(String fileName) {
        return getOssClient().getObject(config.oss_bucket, fileName).getObjectContent();
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
            ObsObject obj = getOssClient().getObject(getBucket(), fileName);
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
        return getOssClient().doesObjectExist(getBucket(), fileName);
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
     * @return 华为的 com.obs.services.model.ObjectMetadata;
     */
    @Override
    public ObjectMetadata getObjectMetadata(String bucket, String remoteFile) {
        return getOssClient().getObjectMetadata(bucket, remoteFile);
    }

    /**
     * @param expireTime 过期时间
     * @param imageParam 转码参数
     */
    @Override
    public URL generatePresignedUrl(String bucket, String fileName, Datetime expireTime, String imageParam) {
        TemporarySignatureRequest request = new TemporarySignatureRequest(HttpMethodEnum.GET,
                expireTime.subtract(DateType.Second, new Datetime()));
        request.setBucketName(bucket);
        request.setObjectKey(fileName);

        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("x-image-process", imageParam);
        request.setQueryParams(queryParams);

        String url = getOssClient().createTemporarySignature(request).getSignedUrl();
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(String.format("链接 %s 生成URL失败", url));
        }
    }

    @Override
    public List<String> listFiles(String bucket) {
        return getOssClient().listObjects(bucket).getObjects().stream().map(item -> item.getObjectKey()).toList();
    }

}
