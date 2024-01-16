package cn.cerc.db.elasticsearch;

import java.io.IOException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.StopWatch;

import cn.cerc.local.tool.JsonTool;

public class ElasticsearchUtilsTest {

    @Before
    public void init() throws IOException {
        ElasticsearchUtils.createIndex(ElasticsearchEntity.class);
    }

//    @Test
    public void append() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("插入1000笔模拟数据");
        for (int i = 0; i < 1000; i++) {
            ElasticsearchEntity entity = ElasticsearchEntity.getRandomData(i);
            IndexResponse indexDocument = ElasticsearchUtils.indexDocument(ElasticsearchEntity.INDEXNAME, entity);
            System.out.println(i + ":" + indexDocument);
        }
        watch.stop();
        System.out.println(watch.prettyPrint());
    }

//    @Test
    public void search() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("模拟查询数据");
        SearchResponse indexDocument = ElasticsearchUtils.searchDocuments(ElasticsearchEntity.INDEXNAME, where -> {
            where.must(QueryBuilders.rangeQuery("age").gt("28"));
        });
        for (int i = 0; i < indexDocument.getHits().getHits().length; i++) {
            SearchHit item = indexDocument.getHits().getHits()[i];
            System.out.println(i + ":" + JsonTool.fromJson(item.getSourceAsString(), ElasticsearchEntity.class));
        }
        watch.stop();
        System.out.println(watch.prettyPrint());
    }

    @Test
    public void update() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("模拟插入修改");
        ElasticsearchEntity entity = ElasticsearchEntity.getRandomData(99999);
        System.out.println("修改前：" + entity);
        IndexResponse indexDocument = ElasticsearchUtils.indexDocument(ElasticsearchEntity.INDEXNAME, entity);

        entity.setName("王五麻子");
        ElasticsearchUtils.updateDocument(ElasticsearchEntity.INDEXNAME, indexDocument.getId(), entity);
        GetResponse document = ElasticsearchUtils.getDocument(ElasticsearchEntity.INDEXNAME, indexDocument.getId());
        System.out.println("修改后：" + JsonTool.fromJson(document.getSourceAsString(), ElasticsearchEntity.class));

        watch.stop();
        System.out.println(watch.prettyPrint());
    }

}
