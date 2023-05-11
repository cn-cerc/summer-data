package cn.cerc.db.mongo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.bson.Document;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.Datetime;

/**
 * 将MongoDB数据集转换为DataSet
 */
public class MongoDataSetConver {
    private DataSet dataSet;
    private Collection<Document> documents;
    private boolean isConverDone;
    /**
     * 时间字段集合：在进行转换时集合内的字段将会被转换为 {@link cn.cerc.db.core.Datetime} 类型
     */
    private Set<String> timeFields;

    public MongoDataSetConver(Collection<Document> documents) {
        this.dataSet = new DataSet();
        this.timeFields = new HashSet<>();
        this.documents = documents;
    }

    public MongoDataSetConver addTimeFields(String... fields) {
        if (fields == null)
            return this;
        for (String field : fields) {
            this.timeFields.add(field);
        }
        return this;
    }

    public DataSet dataSet() {
        if (isConverDone)
            return dataSet;
        synchronized (this) {
            if (isConverDone)
                return dataSet;
            for (Document document : documents) {
                DataRow dataRow = this.dataSet.append().current();
                document.keySet().forEach(key -> {
                    if (timeFields.contains(key))
                        dataRow.setValue(key, new Datetime(document.getLong(key)));
                    else
                        dataRow.setValue(key, document.get(key));
                });
            }
            isConverDone = true;
        }
        return dataSet;
    }

}
