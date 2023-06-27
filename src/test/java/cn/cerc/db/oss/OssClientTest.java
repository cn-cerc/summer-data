package cn.cerc.db.oss;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;

public class OssClientTest {

    private volatile OSS oss = null;

    String endpoint = "";
    String accessKeyId = "";
    String accessKeySecret = "";

    @Before
    public void before() {
        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        // 设置OSSClient使用的最大连接数，默认1024
        conf.setMaxConnections(1024);
        // 设置请求超时时间，默认3秒
        conf.setSocketTimeout(3 * 1000);
        // 设置失败请求重试次数，默认3次
        conf.setMaxErrorRetry(3);

        // 创建OSSClient实例
        oss = new OSSClient(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret), conf);
    }

    @Test
    @Ignore
    public void test() throws IOException {
        // 创建OSSClient实例
        OSSClient client = (OSSClient) oss;
        // 使用访问OSS
        String uuid = UUID.randomUUID().toString();
        // 存储一个对象
        String content = "Hello OSS";
        client.putObject("zrk-oss-test", uuid, new ByteArrayInputStream(content.getBytes()));
        // 获得对象
        OSSObject ossObject = client.getObject("zrk-oss-test", uuid);
        BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;
        }
        // 关闭OSSClient
        client.shutdown();
    }

    @Test
    public void test_copy() {
        String bucket = "4plc-alpha";
        OSSClient client = (OSSClient) oss;
        String sourceFile = "220701/002428a3c57544519bea297e37b85b10.jpg";
        assertTrue(client.doesObjectExist(bucket, sourceFile));

        String targetFile = "temp/220701/002428a3c57544519bea297e37b85b10.jpg";
        client.copyObject(bucket, sourceFile, bucket, targetFile);
        assertTrue(client.doesObjectExist(bucket, targetFile));
    }

    @Test
    public void test_listFiles() {
        String bucket = "vinetest";
        OSSClient client = (OSSClient) oss;
        ObjectListing listObjects = client.listObjects(bucket);
        System.out.println(listObjects.getObjectSummaries());
    }
}
