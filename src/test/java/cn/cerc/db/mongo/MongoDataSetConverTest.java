package cn.cerc.db.mongo;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.Datetime;

public class MongoDataSetConverTest {

    private static final Logger log = LoggerFactory.getLogger(MongoDataSetConverTest.class);

    @Test
    public void test_conver() {
        Datetime now = new Datetime();

        Document document = new Document();
        document.put("create_time_", now.getTimestamp());

        DataSet dataSet = new MongoDataSetConver(List.of(document)).addTimeFields("create_time_").dataSet();
        log.info("转换前时间：{}", now);
        log.info("转换后时间：{}", dataSet.getDatetime("create_time_"));
        assertTrue(dataSet.getDatetime("create_time_").toString().equals(now.toString()));
    }

}
