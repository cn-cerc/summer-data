package cn.cerc.db.mongo;

import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import cn.cerc.core.Datetime;
import cn.cerc.core.ISession;
import cn.cerc.core.DataRow;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.NosqlOperator;

public class MongoOperator implements NosqlOperator {
    private String tableName;
    private MongoDB connection;

    public MongoOperator(ISession session) {
        this.connection = (MongoDB) session.getProperty(MongoDB.SessionId);
    }

    public MongoOperator(IHandle owner) {
        this(owner.getSession());
    }

    @Override
    public boolean insert(DataRow record) {
        MongoCollection<Document> coll = connection.getClient().getCollection(this.tableName);
        Document doc = getValue(record);
        coll.insertOne(doc);
        return true;
    }

    @Override
    public boolean update(DataRow record) {
        MongoCollection<Document> coll = connection.getClient().getCollection(this.tableName);
        Document doc = getValue(record);
        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        UpdateResult res = coll.replaceOne(Filters.eq("_id", key), doc);
        return res.getModifiedCount() == 1;
    }

    @Override
    public boolean delete(DataRow record) {
        MongoCollection<Document> coll = connection.getClient().getCollection(this.tableName);
        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        DeleteResult res = coll.deleteOne(Filters.eq("_id", key));
        return res.getDeletedCount() == 1;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private Document getValue(DataRow record) {
        Document doc = new Document();
        for(String field : record.getFieldDefs().getFields()) {
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
}
