package cn.cerc.db.elasticsearch;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.StopWatch;

public class ElasticsearchTest {

    @Before
    public void init() throws IOException {
        Elasticsearch.createIndex(ElasticsearchEntity.class);
    }

//    @Test
    public void append() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("插入1000笔模拟数据");
        for (int i = 0; i < 1000; i++) {
            ElasticsearchEntity insert = Elasticsearch.insert(ElasticsearchEntity.class,
                    ElasticsearchEntity::setRandomData);
            System.out.println(i + ":" + insert);
        }
        watch.stop();
        System.out.println(watch.prettyPrint());
    }

    @Test
    public void search() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("模拟查询数据");
        Set<ElasticsearchEntity> set = Elasticsearch.search(ElasticsearchEntity.class, where -> {
            // 精准匹配 等于（=）
            // where.must(QueryBuilders.termQuery("_id", "Z3xqFo0BwAvcWAOC93Uo"));
            // IN操作
            // where.must(QueryBuilders.termsQuery("_id", "Z3xqFo0BwAvcWAOC93Uo","name",
            // "王六"));
            // 范围查询
            where.must(QueryBuilders.rangeQuery("age").gte(25));
            // 模糊匹配 会分词匹配
            where.must(QueryBuilders.matchQuery("name", "王六"));
        });
        AtomicInteger counter = new AtomicInteger(0);
        for (var entity : set) {
            System.out.println(counter.incrementAndGet() + ":" + entity);
        }
        watch.stop();
        System.out.println(watch.prettyPrint());
    }

//    @Test
    public void update() throws IOException {
        StopWatch watch = new StopWatch();
        watch.start("模拟插入修改");
        ElasticsearchEntity insert = Elasticsearch.insert(ElasticsearchEntity.class,
                ElasticsearchEntity::setRandomData);
        System.out.println("修改前：" + insert);
        ElasticsearchEntity update = Elasticsearch.update(ElasticsearchEntity.class, insert.getId(), item -> {
            item.setName("王五麻子");
        });
        System.out.println("修改后：" + update);
        watch.stop();
        System.out.println(watch.prettyPrint());
    }

}
