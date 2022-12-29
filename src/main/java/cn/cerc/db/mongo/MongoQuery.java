package cn.cerc.db.mongo;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataRowState;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.FieldMeta;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;

public class MongoQuery extends DataSet implements IHandle {
    private static final long serialVersionUID = -1262005194419604476L;
    private static final ClassResource res = new ClassResource(MongoQuery.class, SummerDB.ID);
    private ISession session;
    private boolean active;
    private final SqlText sql = new SqlText(SqlServerType.Mysql);
    private int limit = MAX_RECORDS;
    // 从数据库每次加载的最大笔数
    public static final int MAX_RECORDS = 50000;

    public MongoQuery(IHandle handle) {
        super();
        this.session = handle.getSession();
    }

    public MongoQuery open() {
        this.setStorage(true);
        // 查找业务ID对应的数据
        MongoClient client = MongoConfig.getClient();
        MongoDatabase database = client.getDatabase(MongoConfig.database());
        MongoCollection<Document> collection = database.getCollection(collectionName());
        // 增加查询条件
        BasicDBObject filter = decodeWhere(this.sql().text());
        // 增加排序条件
        BasicDBObject sort = decodeOrder(this.sql().text());
        // 执行查询
        FindIterable<Document> findIterable = collection.find(filter).sort(sort).limit(limit);
        ArrayList<Document> list = findIterable.into(new ArrayList<>());
        // 数据不存在,则状态不为更新,并返回一个空数据
        if (list.isEmpty())
            return this;

        for (Document doc : list) {
            DataRow record = append().current();
            for (String field : doc.keySet()) {
                if ("_id".equals(field)) {
                    Object uid = doc.get(field);
                    record.setValue(field, uid != null ? uid.toString() : uid);
                } else {
                    record.setValue(field, doc.get(field));
                }
            }
            record.setState(DataRowState.None);
        }
        this.first();
        this.active = true;
        return this;
    }

    private String collectionName() {
        String collectionName = SqlText.findTableName(this.sql().text());
        if (Utils.isEmpty(collectionName))
            throw new RuntimeException("Mongo table can not be empty.");
        return collectionName;
    }

    // 将sql指令查询条件改为MongoDB格式
    protected BasicDBObject decodeWhere(String sql) {
        BasicDBObject filter = new BasicDBObject();
        int offset = sql.toLowerCase().indexOf("where");
        if (offset > -1) {
            int endIndex = sql.toLowerCase().indexOf("order");
            String[] items;
            if (endIndex > -1) {
                items = sql.substring(offset + 5, endIndex).split(" and ");
            } else {
                items = sql.substring(offset + 5).split(" and ");
            }
            for (String item : items) {
                if (item.split(">=").length == 2) {
                    setCondition(filter, item, ">=");
                } else if (item.split("<=").length == 2) {
                    setCondition(filter, item, "<=");
                } else if (item.split("<>").length == 2) {
                    setCondition(filter, item, "<>");
                } else if (item.split("=").length == 2) {
                    setCondition(filter, item, "=");
                } else if (item.split(">").length == 2) {
                    setCondition(filter, item, ">");
                } else if (item.split("<").length == 2) {
                    setCondition(filter, item, "<");
                } else if (item.split("like").length == 2) {
                    String[] tmp = item.split("like");
                    String field = tmp[0].trim();
                    String value = tmp[1].trim();
                    if (value.startsWith("'") && value.endsWith("'")) {
                        // 不区分大小写的模糊搜索
                        Pattern queryPattern = Pattern.compile(value.substring(1, value.length() - 1),
                                Pattern.CASE_INSENSITIVE);
                        filter.append(field, queryPattern);
                    } else {
                        throw new RuntimeException(String.format(res.getString(1, "模糊查询条件：%s 必须为字符串"), item));
                    }
                } else if (item.split("in").length == 2) {
                    String[] tmp = item.split("in");
                    String field = tmp[0].trim();
                    String value = tmp[1].trim();
                    if (value.startsWith("(") && value.endsWith(")")) {
                        BasicDBList values = new BasicDBList();
                        for (String str : value.substring(1, value.length() - 1).split(",")) {
                            if (str.startsWith("'") && str.endsWith("'")) {
                                values.add(str.substring(1, str.length() - 1));
                            } else {
                                values.add(str);
                            }
                        }
                        filter.put(field, new BasicDBObject("$in", values));
                    } else {
                        throw new RuntimeException(String.format(res.getString(2, "in查询条件：%s 必须有带有()"), item));
                    }
                } else {
                    throw new RuntimeException(String.format(res.getString(3, "暂不支持的查询条件：%s"), item));
                }
            }
        }
        return filter;
    }

    private void setCondition(BasicDBObject filter, String item, String symbol) {
        Map<String, String> compare = new HashMap<>();
        compare.put("=", "$eq");
        compare.put("<>", "$ne");
        compare.put(">", "$gt");
        compare.put(">=", "$gte");
        compare.put("<", "$lt");
        compare.put("<=", "$lte");
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = tmp[1].trim();
        if (filter.get(field) != null) {
            if (value.startsWith("'") && value.endsWith("'")) {
                ((BasicDBObject) filter.get(field)).append(compare.get(symbol), value.substring(1, value.length() - 1));
            } else if (Utils.isNumeric(value)) {
                ((BasicDBObject) filter.get(field)).append(compare.get(symbol), Double.parseDouble(value));
            } else {
                ((BasicDBObject) filter.get(field)).append(compare.get(symbol), value);
            }
        } else {
            if (value.startsWith("'") && value.endsWith("'")) {
                filter.put(field, new BasicDBObject(compare.get(symbol), value.substring(1, value.length() - 1)));
            } else if (Utils.isNumeric(value)) {
                filter.put(field, new BasicDBObject(compare.get(symbol), Double.parseDouble(value)));
            } else {
                filter.put(field, new BasicDBObject(compare.get(symbol), value));
            }
        }
    }

    // 将sql指令排序条件改为MongoDB格式
    protected BasicDBObject decodeOrder(String sql) {
        BasicDBObject sort = new BasicDBObject();
        int offset = sql.toLowerCase().indexOf("order");
        if (offset == -1) {
            return sort;
        }
        String[] items = sql.substring(offset + 5).split(",");
        for (String item : items) {
            String str = item.trim();
            if (str.split(" ").length == 2) {
                String[] tmp = str.split(" ");
                if ("ASC".equals(tmp[1])) {
                    sort.append(tmp[0], 1);
                } else if ("DESC".equals(tmp[1])) {
                    sort.append(tmp[0], -1);
                } else {
                    throw new RuntimeException("暂不支持的排序条件：" + str);
                }
            } else {
                sort.append(str, 1);
            }
        }
        return sort;
    }

    private Document getValue(DataRow record) {
        Document doc = new Document();
        for (String field : record.fields().names()) {
            if ("_id".equals(field)) {
                continue;
            }
            Object obj = record.getValue(field);
            if (obj instanceof Date) {
                doc.append(field, (new Datetime((Date) obj)).toString());
            } else {
                doc.append(field, obj);
            }
        }
        return doc;
    }

    @Override
    protected final void insertStorage(DataRow record) {
        String collectionName = collectionName();
        MongoClient client = MongoConfig.getClient();
        MongoDatabase database = client.getDatabase(MongoConfig.database());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document doc = getValue(record);
        collection.insertOne(doc);
    }

    @Override
    protected final void updateStorage(DataRow record) {
        MongoClient client = MongoConfig.getClient();
        MongoDatabase database = client.getDatabase(MongoConfig.database());
        MongoCollection<Document> collection = database.getCollection(collectionName());
        Document doc = getValue(record);
        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        UpdateResult res = collection.replaceOne(Filters.eq("_id", key), doc);
        if (res.getModifiedCount() != 1)
            throw new RuntimeException("MongoDB update error");
    }

    @Override
    protected final void deleteStorage(DataRow record) {
        MongoClient client = MongoConfig.getClient();
        MongoDatabase database = client.getDatabase(MongoConfig.database());
        MongoCollection<Document> collection = database.getCollection(collectionName());

        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        DeleteResult res = collection.deleteOne(Filters.eq("_id", key));
        if (res.getDeletedCount() == 1)
            garbage().remove(record);
    }

    // 将通用类型，转成DataSet，方便操作
    public DataSet getChildDataSet(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?>)) {
            throw new RuntimeException("错误的数据类型！");
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) value;
        DataSet dataSet = new DataSet();
        for (Map<String, Object> item : items) {
            DataRow record = dataSet.append().current();
            for (String key : item.keySet()) {
                record.setValue(key, item.get(key));
            }
            record.setState(DataRowState.None);
        }
        return dataSet;
    }

    // 将DataSet转成通用类型，方便存入MongoDB
    public void setChildDataSet(String field, DataSet dataSet) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (DataRow child : dataSet.records()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (FieldMeta meta : child.fields())
                map.put(meta.code(), child.getValue(meta.code()));
            items.add(map);
        }
        this.setValue(field, items);
    }

    @SuppressWarnings("unchecked")
    public List<Object> assignList(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            List<Object> items = new ArrayList<>();
            this.setValue(field, items);
            return items;
        }
        if (!(value instanceof List<?>)) {
            throw new RuntimeException("错误的数据类型！");
        }
        return (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    public Set<Object> assignSet(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            Set<Object> items = new LinkedHashSet<>();
            this.setValue(field, items);
            return items;
        }
        if ((value instanceof Set<?>) || !(value instanceof List<?>))
            throw new RuntimeException("错误的数据类型！");

        List<Object> list = (List<Object>) value;
        return new LinkedHashSet<>(list);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> assignMap(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            Map<String, Object> items = new LinkedHashMap<>();
            this.setValue(field, items);
            return items;
        }
        if (!(value instanceof List<?>)) {
            throw new RuntimeException("错误的数据类型！");
        }
        return (Map<String, Object>) value;
    }

    public MongoQuery add(String sqlText) {
        this.sql.add(sqlText);
        return this;
    }

    public MongoQuery add(String format, Object... args) {
        this.sql.add(format, args);
        return this;
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public SqlText sql() {
        return this.sql;
    }

    @Deprecated
    public final SqlText getSqlText() {
        return this.sql();
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public MongoQuery setJson(String json) {
        super.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

    public void setMaximum(int limit) {
        if (limit <= 0)
            throw new RuntimeException("limit 不允许小于等于0");
        if (limit > MAX_RECORDS)
            throw new RuntimeException(String.format("limit 不支持大于 %d", MAX_RECORDS));
        this.limit = limit;
    }

}
