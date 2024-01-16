package cn.cerc.db.elasticsearch;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;
import cn.cerc.db.elasticsearch.annotation.Document;
import cn.cerc.db.elasticsearch.annotation.Field;
import cn.cerc.local.tool.JsonTool;

public class ElasticsearchUtils {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchUtils.class);

    private static final RestHighLevelClient CLIENT = ElasticsearchClient.getClient();

    public static boolean createIndex(Class<?> classz) throws IOException {
        Document document = classz.getAnnotation(Document.class);
        String indexName = document != null ? document.indexName() : classz.getSimpleName();

        // 创建一个GetIndexRequest实例
        GetIndexRequest request1 = new GetIndexRequest(indexName);
        // 使用RestHighLevelClient执行请求
        GetIndexResponse response1 = CLIENT.indices().get(request1, RequestOptions.DEFAULT);
        // 检查响应中是否包含索引
        boolean indexExists = response1.getIndices().length > 0;
        if (indexExists)
            return indexExists;

        IndicesClient indicesClient = CLIENT.indices();
        CreateIndexRequest request2 = new CreateIndexRequest(indexName);

        Map<String, Object> doc = new LinkedHashMap<>();
        for (java.lang.reflect.Field field : classz.getDeclaredFields()) {
            Field annotation = field.getAnnotation(Field.class);
            if (annotation == null)
                continue;
            Map<String, Object> fieldProperties = new LinkedHashMap<>();
            fieldProperties.put("type", annotation.type().name().toLowerCase());
            if (Utils.isNotEmpty(annotation.analyzer()))
                fieldProperties.put("analyzer", annotation.analyzer());
            if (Utils.isNotEmpty(annotation.format()))
                fieldProperties.put("format", annotation.format());
            doc.put(field.getName(), fieldProperties);
        }
        request2.mapping(JsonTool.toJson(Map.of("properties", doc)), XContentType.JSON);
        try {
            CreateIndexResponse response2 = indicesClient.create(request2, RequestOptions.DEFAULT);
            log.info("创建索引 {} 成功", indexName);
            return response2.isAcknowledged();
        } catch (Exception e) {
            log.info("创建索引 {} 失败 原因：{}", indexName, e.getMessage(), e);
            return false;
        }
    }

    public static <T> IndexResponse indexDocument(String indexName, T document) {
        try {
            IndexRequest request = new IndexRequest(indexName);
            request.source(formatJson(document), XContentType.JSON);
            return CLIENT.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    public static GetResponse getDocument(String indexName, String id) throws IOException {
        return CLIENT.get(new GetRequest(indexName, id), RequestOptions.DEFAULT);
    }

    public static <T> UpdateResponse updateDocument(String indexName, String id, T document) throws IOException {
        UpdateRequest request = new UpdateRequest(indexName, id).doc(formatJson(document), XContentType.JSON);
        return CLIENT.update(request, RequestOptions.DEFAULT);
    }

    public DeleteResponse deleteDocument(String indexName, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(indexName, id);
        return CLIENT.delete(request, RequestOptions.DEFAULT);
    }

    public static SearchResponse searchDocuments(String indexName, Consumer<BoolQueryBuilder> where)
            throws IOException {
        Objects.requireNonNull(where);
        // 构建查询条件
        BoolQueryBuilder matchAllQueryBuilder = QueryBuilders.boolQuery();
        where.accept(matchAllQueryBuilder);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(1000);
        // 执行查询
        SearchResponse searchResponse = CLIENT.search(
                new SearchRequest(indexName).source(builder.query(matchAllQueryBuilder)), RequestOptions.DEFAULT);
        return searchResponse;
    }

    private static <T> String formatJson(T document) {
        DataRow row = new DataRow();
        for (var field : document.getClass().getDeclaredFields()) {
            if (field.getModifiers() == Modifier.STATIC)
                continue;
            field.setAccessible(true);
            try {
                row.setValue(field.getName(), field.get(document));
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return row.json();
    }
}
