package cn.cerc.db.oss;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.oss.OSS;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.PutObjectResult;

import cn.cerc.db.core.Datetime;

public class ObsClientTest {

    private volatile OSS oss = null;

    String endPoint = "https://obs.cn-east-3.myhuaweicloud.com";
    String ak = "QDONBE4CTM7Q77O5KM4V";
    String sk = "2z2GeIVVpHjEAAoBbnOQbcC8QhuWzR11vM8K0yJJ";
    String bucket = "4plc-alpha";

    @Before
    public void before() {
    }

    @Test
    public void test_listBuckets() throws IOException {
        // 创建ObsClient实例
        try (ObsClient obsClient = new ObsClient(ak, sk, endPoint)) {

            // 列举桶
            ListBucketsRequest request = new ListBucketsRequest();
            request.setQueryLocation(true);
            List<ObsBucket> buckets = obsClient.listBuckets(request);
            for (ObsBucket bucket : buckets) {
                System.out.printf("===== 桶名称:%s =====\n", bucket.getBucketName());
                System.out.println("创建时间:" + new Datetime(bucket.getCreationDate()));
                System.out.println("区域:" + bucket.getLocation());
            }
        }
    }

    @Test
    public void test_upload() {
        try (ObsClient obsClient = new ObsClient(ak, sk, endPoint)) {
            // 方式1: localfile为待上传的本地文件路径，需要指定到具体的文件名
            PutObjectResult putResult1 = obsClient.putObject(bucket, "/test/firstFile",
                    new File("/home/admin/Pictures/Wallpapers/desktop.jpg"));

            System.out.println(putResult1.getStatusCode());

            // 方式2: localfile2 为待上传的本地文件路径，需要指定到具体的文件名
//        PutObjectRequest request = new PutObjectRequest();
//        request.setBucketName(bucket);
//        request.setObjectKey("objectkey2");
//        request.setFile(new File("localfile2"));
//        PutObjectResult putResult2 = obsClient.putObject(request);
//        System.out.println(putResult2.getStatusCode());
        } catch (ObsException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_listObjs() throws IOException {
        try (ObsClient obsClient = new ObsClient(ak, sk, endPoint)) {
            ObjectListing objects = obsClient.listObjects(bucket);
            objects.getObjects().forEach(item -> System.out.println(item.getObjectKey()));
        }
    }
}
