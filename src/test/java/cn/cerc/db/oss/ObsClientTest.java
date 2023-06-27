package cn.cerc.db.oss;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.PutObjectResult;

import cn.cerc.db.core.Datetime;

public class ObsClientTest {

    // 地藤
    private static final String endPoint = "";
    private static final String ak = "";
    private static final String sk = "";
    private static final String bucket = "";

    @Before
    public void before() {
    }

    @Test
    public void test_listBuckets() throws IOException {
        // 创建ObsClient实例
        try (ObsClient obsClient = new ObsClient(ak, sk, endPoint)) {
            ListBucketsRequest request = new ListBucketsRequest();
//            request.setQueryLocation(true);
            // 列举桶
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
            PutObjectResult putResult1 = obsClient.putObject(bucket, "/test/firstFile1",
                    new File("/home/admin/Pictures/Wallpapers/desktop.jpg"));

            assertEquals(putResult1.getStatusCode(), 200);
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
