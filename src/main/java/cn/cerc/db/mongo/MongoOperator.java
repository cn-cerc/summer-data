package cn.cerc.db.mongo;

import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Datetime;

public class MongoOperator implements AutoCloseable {
    private MongoConfig connection;
    private MongoCollection<Document> collection;

    public MongoOperator(String collectionName) {
        super();
        connection = new MongoConfig();
        MongoDatabase db = connection.getClient();
        BasicDBObject bson = BasicDBObject.parse("db.serverStatus()");
        Document result = db.runCommand(bson);
        System.out.println(result);
        collection = db.getCollection(collectionName);
    }

    public boolean insert(DataRow record) {
        Document doc = getValue(record);
        collection.insertOne(doc);
        return true;
    }

    public boolean update(DataRow record) {
        Document doc = getValue(record);
        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        UpdateResult res = collection.replaceOne(Filters.eq("_id", key), doc);
        return res.getModifiedCount() == 1;
    }

    public boolean delete(DataRow record) {
        String uid = record.getString("_id");
        Object key = "".equals(uid) ? "null" : new ObjectId(uid);
        DeleteResult res = collection.deleteOne(Filters.eq("_id", key));
        return res.getDeletedCount() == 1;
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
    public void close() {
        this.connection.close();
        this.collection = null;
    }
}
