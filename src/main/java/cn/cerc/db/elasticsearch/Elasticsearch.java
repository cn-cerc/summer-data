package cn.cerc.db.elasticsearch;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.persistence.Id;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;
import cn.cerc.db.elasticsearch.annotation.Document;
import cn.cerc.local.tool.JsonTool;

public class Elasticsearch {

    private static final Logger log = LoggerFactory.getLogger(Elasticsearch.class);

    private static final RestHighLevelClient CLIENT = ElasticsearchClient.getClient();

    private static final Map<Class<?>, Field> FIELDBUFF = new ConcurrentHashMap<>();

    public static boolean createIndex(Class<?> classz) throws IOException {
        Document document = classz.getAnnotation(Document.class);
        String indexName = document != null ? document.indexName() : classz.getSimpleName();

        IndicesClient indicesClient = CLIENT.indices();
        CreateIndexRequest request2 = new CreateIndexRequest(indexName);
        Map<String, Object> doc = new LinkedHashMap<>();
        for (Field field : classz.getDeclaredFields()) {
            cn.cerc.db.elasticsearch.annotation.Field annotation = field
                    .getAnnotation(cn.cerc.db.elasticsearch.annotation.Field.class);
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
            log.info("创建索引 {} 失败 原因：{}", indexName, e.getMessage());
            return false;
        }
    }

    public static <T> T insert(Class<T> classz, Consumer<T> consumer) {
        Objects.requireNonNull(classz);
        Objects.requireNonNull(consumer);
        try {
            T entity = classz.getDeclaredConstructor().newInstance();
            consumer.accept(entity);

            Field idField = null;
            DataRow row = new DataRow();
            for (var field : classz.getDeclaredFields()) {
                if (field.getModifiers() == Modifier.STATIC)
                    continue;
                if (idField == null && field.getAnnotation(Id.class) != null)
                    idField = field;
                field.setAccessible(true);
                try {
                    row.setValue(field.getName(), field.get(entity));
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            if (idField == null)
                throw new RuntimeException(String.format("class %s 没有字段标识Id注解", classz.getSimpleName()));

            Document document = classz.getAnnotation(Document.class);
            if (document == null || Utils.isEmpty(document.indexName()))
                throw new RuntimeException(String.format("class %s 没有Document注解", classz.getSimpleName()));

            IndexRequest request = new IndexRequest(document.indexName());
            request.source(row.json(), XContentType.JSON);
            IndexResponse index = CLIENT.index(request, RequestOptions.DEFAULT);
            if (index.getResult() != Result.CREATED)
                throw new RuntimeException("创建文档失败");
            // 回写到id字段上
            idField.set(entity, index.getId());
            return entity;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Optional<T> get(Class<T> classz, String id) {
        Document document = classz.getAnnotation(Document.class);
        if (document == null || Utils.isEmpty(document.indexName()))
            throw new RuntimeException(String.format("class %s 没有Document注解", classz.getSimpleName()));
        try {
            GetResponse response = CLIENT.get(new GetRequest(document.indexName(), id), RequestOptions.DEFAULT);
            if (response.isExists()) {
                T entity = JsonTool.fromJson(response.getSourceAsString(), classz);
                getIdField(classz).set(entity, response.getId());
                return Optional.ofNullable(entity);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * 先根据ID查后修改
     * 
     * @param classz 实体类必须标识Document
     * @param id     唯一标识
     * @return 返回修改过后的实体
     */
    public static <T> T update(Class<T> classz, String id, Consumer<T> consumer) {
        Objects.requireNonNull(consumer);
        Document document = classz.getAnnotation(Document.class);
        if (document == null || Utils.isEmpty(document.indexName()))
            throw new RuntimeException(String.format("class %s 没有Document注解", classz.getSimpleName()));
        Optional<T> entityOp = get(classz, id);
        if (entityOp.isEmpty())
            throw new RuntimeException(String.format("%s is empty", id));
        T entity = entityOp.get();
        consumer.accept(entity);
        return update(document.indexName(), id, entity);
    }

    /***
     * 
     * 直接根据ID修改
     * 
     * @param indexName 索引名称
     * @param id        唯一标识
     * @return 返回修改过后的实体
     */
    public static <T> T update(String indexName, String id, T entity) {
        Objects.requireNonNull(indexName);
        Objects.requireNonNull(id);
        try {
            DataRow row = new DataRow();
            for (var field : entity.getClass().getDeclaredFields()) {
                if (field.getModifiers() == Modifier.STATIC)
                    continue;
                field.setAccessible(true);
                try {
                    row.setValue(field.getName(), field.get(entity));
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            UpdateRequest request = new UpdateRequest(indexName, id).doc(row.json(), XContentType.JSON);
            CLIENT.update(request, RequestOptions.DEFAULT);
            return entity;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean delete(String indexName, String id) throws IOException {
        Objects.requireNonNull(indexName);
        Objects.requireNonNull(id);
        DeleteRequest request = new DeleteRequest(indexName, id);
        DeleteResponse delete = CLIENT.delete(request, RequestOptions.DEFAULT);
        return delete.getResult() == Result.DELETED;
    }

    public static <T> Set<T> search(Class<T> classz, Consumer<BoolQueryBuilder> where) throws IOException {
        Objects.requireNonNull(classz);
        Objects.requireNonNull(where);

        Document document = classz.getAnnotation(Document.class);
        if (document == null || Utils.isEmpty(document.indexName()))
            throw new RuntimeException(String.format("class %s 没有Document注解", classz.getSimpleName()));
        // 构建查询条件
        BoolQueryBuilder matchAllQueryBuilder = QueryBuilders.boolQuery();
        where.accept(matchAllQueryBuilder);

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(5000);
        // 执行查询
        SearchResponse searchResponse = CLIENT.search(
                new SearchRequest(document.indexName()).source(builder.query(matchAllQueryBuilder)),
                RequestOptions.DEFAULT);

        Set<T> set = new HashSet<>();
        for (var hit : searchResponse.getHits().getHits()) {
            T entity = JsonTool.fromJson(hit.getSourceAsString(), classz);
            try {
                getIdField(classz).set(entity, hit.getId());
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
            set.add(entity);
        }
        return set;
    }

    public static Field getIdField(Class<?> classz) {
        if (FIELDBUFF.containsKey(classz))
            return FIELDBUFF.get(classz);
        Field idField = null;
        for (var field : classz.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null) {
                field.setAccessible(true);
                idField = field;
                break;
            }
        }
        if (idField == null)
            throw new RuntimeException(String.format("class %s 没有字段标识Id注解", classz.getSimpleName()));
        FIELDBUFF.put(classz, idField);
        return idField;
    }

}
