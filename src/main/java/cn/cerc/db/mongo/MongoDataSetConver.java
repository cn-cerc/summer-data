package cn.cerc.db.mongo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

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
    private Map<String, Function<Object, Object>> converFunction;

    public MongoDataSetConver(Collection<Document> documents) {
        this.dataSet = new DataSet();
        this.converFunction = new HashMap<>();
        this.documents = documents;
    }

    public MongoDataSetConver addTimeFields(String... fields) {
        if (fields == null)
            return this;
        for (String field : fields) {
            this.addConverFunction(field, DatetimeConver.INSTANCE);
        }
        return this;
    }

    public MongoDataSetConver addConverFunction(String field, Function<Object, Object> func) {
        this.converFunction.put(field, func);
        return this;
    }

    public DataSet dataSet() {
        if (!isConverDone) {
            synchronized (this) {
                if (!isConverDone) {
                    for (Document document : documents) {
                        DataRow dataRow = this.dataSet.append().current();
                        document.forEach((key, value) -> dataRow.setValue(key,
                                converFunction.getOrDefault(key, DefaultConver.INSTANCE).apply(value)));
                    }
                    isConverDone = true;
                }
            }
        }
        dataSet.first();
        return dataSet;
    }

    protected enum DatetimeConver implements Function<Object, Object> {
        INSTANCE;

        @Override
        public Object apply(Object t) {
            return new Datetime((Long) t);
        }
    }

    protected enum DefaultConver implements Function<Object, Object> {
        INSTANCE;

        @Override
        public Object apply(Object t) {
            return t;
        }
    }

}
