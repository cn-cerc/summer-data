package cn.cerc.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import cn.cerc.core.FieldMeta.FieldType;

public class DataSet implements IRecord, Serializable, Iterable<Record> {
    private static final Logger log = LoggerFactory.getLogger(DataSet.class);
    private static final long serialVersionUID = 873159747066855363L;
    private static final ClassResource res = new ClassResource(DataSet.class, SummerCore.ID);
    private int recNo = 0;
    private int fetchNo = -1;
    private int state = 0;
    private String message = null;
    private FieldDefs fieldDefs = new FieldDefs();
    private List<Record> records = new ArrayList<Record>();
    private DataSetEvent appendEvent;
    private RecordEvent beforePostEvent;
    private RecordEvent afterPostEvent;
    private RecordEvent beforeDeleteEvent;
    private RecordEvent afterDeleteEvent;
    private boolean readonly;
    private Record head = null;
    private FieldDefs head_defs = null;
    // 在变更时，是否需要同步保存到数据库中
    private boolean storage;
    // 批次保存模式，默认为post与delete立即保存
    private boolean batchSave = false;
    // 仅当batchSave为true时，delList才有记录存在
    protected List<Record> delList = new ArrayList<>();

    public DataSet() {
        super();
    }

    public DataSet(String jsonData) {
        super();
        this.setJSON(jsonData);
    }

    protected Record newRecord() {
        Record record = new Record(this.fieldDefs);
        record.setDataSet(this);
        record.setState(RecordState.dsInsert);
        return record;
    }

    public DataSet append() {
        return append(newRecord());
    }

    public DataSet append(Record record) {
        this.records.add(record);
        recNo = records.size();
        doAppend(record);
        return this;
    }

    public DataSet append(int index) {
        Record record = newRecord();
        if (index == -1 || index == records.size()) {
            this.records.add(record);
            recNo = records.size();
        } else {
            this.records.add(index, record);
            recNo = index + 1;
        }
        doAppend(record);
        return this;
    }

    public final void edit() {
        if (bof() || eof()) {
            throw new RuntimeException(res.getString(1, "当前记录为空，无法修改"));
        }
        this.getCurrent().setState(RecordState.dsEdit);
    }

    public final void delete() {
        if (bof() || eof())
            throw new RuntimeException(res.getString(2, "当前记录为空，无法删除"));

        Record record = this.getCurrent();
        records.remove(recNo - 1);
        if (this.fetchNo > -1) {
            this.fetchNo--;
        }
        if (record.getState() == RecordState.dsInsert) {
            return;
        }
        if (this.isBatchSave()) {
            delList.add(record);
        } else {
            doBeforeDelete(record);
            if (this.isStorage()) {
                try {
                    deleteStorage(record);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterDelete(record);
        }
    }

    public final void post() {
        if (this.isBatchSave())
            return;

        Record record = this.getCurrent();
        if (record.getState() == RecordState.dsInsert) {
            doBeforePost(record);
            if (this.isStorage()) {
                try {
                    insertStorage(record);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterPost(record);
        } else if (record.getState() == RecordState.dsEdit) {
            doBeforePost(record);
            if (this.isStorage()) {
                try {
                    updateStorage(record);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterPost(record);
        }
    }

    protected void insertStorage(Record record) throws Exception {

    }

    protected void updateStorage(Record record) throws Exception {

    }

    protected void deleteStorage(Record record) throws Exception {

    }

    public boolean first() {
        if (records.size() > 0) {
            this.recNo = 1;
        } else {
            this.recNo = 0;
        }
        fetchNo = -1;
        return this.recNo > 0;
    }

    public boolean last() {
        this.recNo = this.records.size();
        return this.recNo > 0;
    }

    public boolean prior() {
        if (this.recNo > 0) {
            this.recNo--;
        }
        return this.recNo > 0;
    }

    public boolean next() {
        if (this.records.size() > 0 && recNo <= this.records.size()) {
            recNo++;
            return true;
        } else {
            return false;
        }
    }

    public boolean bof() {
        return this.recNo == 0;
    }

    public boolean eof() {
        return this.records.size() == 0 || this.recNo > this.records.size();
    }

    public Record getCurrent() {
        if (this.eof()) {
            throw new RuntimeException(String.format("[%s]eof == true", this.getClass().getName()));
        } else if (this.bof()) {
            throw new RuntimeException(String.format("[%s]bof == true", this.getClass().getName()));
        } else {
            return records.get(recNo - 1);
        }
    }

    public List<Record> getRecords() {
        return records;
    }

    public Record getIndex(int index) {
        this.setRecNo(index + 1);
        return this.getCurrent();
    }

    public int getRecNo() {
        return recNo;
    }

    public void setRecNo(int recNo) {
        if (recNo > this.records.size()) {
            String msg = String.format(res.getString(3, "[%s]RecNo %d 大于总长度 %d"), this.getClass().getName(), recNo,
                    this.records.size());
            throw new RuntimeException(msg);
        } else {
            this.recNo = recNo;
        }
    }

    public int size() {
        return this.records.size();
    }

    public FieldDefs getFieldDefs() {
        return this.fieldDefs;
    }

    // 仅用于查找一次时，调用此函数，速度最快
    public boolean locateOnlyOne(String fields, Object... values) {
        if (fields == null || "".equals(fields)) {
            throw new RuntimeException(res.getString(4, "参数名称不能为空"));
        }
        if (values == null || values.length == 0) {
            throw new RuntimeException(res.getString(5, "值列表不能为空或者长度不能为0"));
        }
        String[] fieldslist = fields.split(";");
        if (fieldslist.length != values.length) {
            throw new RuntimeException(res.getString(6, "参数名称与值列表长度不匹配"));
        }
        Map<String, Object> fieldValueMap = new HashMap<String, Object>();
        for (int i = 0; i < fieldslist.length; i++) {
            fieldValueMap.put(fieldslist[i], values[i]);
        }

        this.first();
        while (this.fetch()) {
            if (this.getCurrent().equalsValues(fieldValueMap)) {
                return true;
            }
        }
        return false;
    }

    // 用于查找多次，调用时，会先进行排序，以方便后续的相同Key查找
    public boolean locate(String fields, Object... values) {
        SearchDataSet search = new SearchDataSet(this);
        search.setFields(fields);
        Record record = values.length == 1 ? search.get(values[0]) : search.get(values);

        if (record == null) {
            return false;
        }
        this.setRecNo(this.records.indexOf(record) + 1);
        return true;
    }

    public Record lookup(String fields, Object... values) {
        SearchDataSet search = new SearchDataSet(this);
        search.setFields(fields);
        return values.length == 1 ? search.get(values[0]) : search.get(values);
    }

    @Override
    public Object getField(String field) {
        return this.getCurrent().getField(field);
    }

    // 排序
    public void setSort(String... fields) {
        Collections.sort(this.getRecords(), new RecordComparator(fields));
    }

    public void setSort(Comparator<Record> func) {
        Collections.sort(this.getRecords(), func);
    }

    @Override
    public String getString(String field) {
        return this.getCurrent().getString(field);
    }

    @Override
    public double getDouble(String field) {
        return this.getCurrent().getDouble(field);
    }

    @Override
    public boolean getBoolean(String field) {
        return this.getCurrent().getBoolean(field);
    }

    @Override
    public int getInt(String field) {
        return this.getCurrent().getInt(field);
    }

    @Override
    public BigInteger getBigInteger(String field) {
        return this.getCurrent().getBigInteger(field);
    }

    @Override
    public BigDecimal getBigDecimal(String field) {
        return this.getCurrent().getBigDecimal(field);
    }

    @Override
    public TDate getDate(String field) {
        return this.getCurrent().getDate(field);
    }

    @Override
    public TDateTime getDateTime(String field) {
        return this.getCurrent().getDateTime(field);
    }

    @Override
    public DataSet setField(String field, Object value) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        this.getCurrent().setField(field, value);
        return this;
    }

    @Deprecated
    public DataSet setNull(String field) {
        this.getCurrent().setField(field, null);
        return this;
    }

    public boolean fetch() {
        boolean result = false;
        if (this.fetchNo < (this.records.size() - 1)) {
            this.fetchNo++;
            this.setRecNo(this.fetchNo + 1);
            result = true;
        }
        return result;
    }

    public void copyRecord(Record source, FieldDefs defs) {
        this.getCurrent().copyValues(source, defs);
    }

    public void copyRecord(Record source, String... fields) {
        this.getCurrent().copyValues(source, fields);
    }

    public void copyRecord(Record sourceRecord, String[] sourceFields, String[] targetFields) {
        if (targetFields.length != sourceFields.length) {
            throw new RuntimeException(res.getString(7, "前后字段数目不一样，请您确认！"));
        }
        Record targetRecord = this.getCurrent();
        for (int i = 0; i < sourceFields.length; i++) {
            targetRecord.setField(targetFields[i], sourceRecord.getField(sourceFields[i]));
        }
    }

    public boolean isNull(String field) {
        Object obj = getCurrent().getField(field);
        return obj == null || "".equals(obj);
    }

    @Override
    public Iterator<Record> iterator() {
        return records.iterator();
    }

    @Override
    public boolean exists(String field) {
        return this.getFieldDefs().exists(field);
    }

    // 用于设置字段的初始值，如 pid, it 等
    public final void onAppend(DataSetEvent appendEvent) {
        this.appendEvent = appendEvent;
    }

    public final DataSetEvent getOnAppend() {
        return appendEvent;
    }

    protected final void doAppend(Record record) {
        if (appendEvent != null)
            appendEvent.execute(this);
    }

    // 用于在保存之前检查字段值是否合法，若失败则抛异常，以及给updateTime赋值
    public final void onBeforePost(RecordEvent beforePostEvent) {
        this.beforePostEvent = beforePostEvent;
    }

    public final RecordEvent getBeforePost() {
        return beforePostEvent;
    }

    protected final void doBeforePost(Record record) {
        if (beforePostEvent != null)
            beforePostEvent.execute(record);
    }

    // 用于在保存成功之后进行记录
    public final void onAfterPost(RecordEvent afterPostEvent) {
        this.afterPostEvent = afterPostEvent;
    }

    public final RecordEvent getAfterPost() {
        return afterPostEvent;
    }

    protected final void doAfterPost(Record record) {
        if (afterPostEvent != null)
            afterPostEvent.execute(record);
        record.setState(RecordState.dsNone);
    }

    // 用于在实际删除前检查是否允许删除
    public final void onBeforeDelete(RecordEvent beforeDeleteEvent) {
        this.beforeDeleteEvent = beforeDeleteEvent;
    }

    public final RecordEvent getBeforeDelete() {
        return beforeDeleteEvent;
    }

    protected final void doBeforeDelete(Record record) {
        if (beforeDeleteEvent != null)
            beforeDeleteEvent.execute(record);
    }

    // 用于在删除成功后进行记录
    public final void onAfterDelete(RecordEvent afterDeleteEvent) {
        this.afterDeleteEvent = afterDeleteEvent;
    }

    public final RecordEvent getAfterDelete() {
        return afterDeleteEvent;
    }

    protected final void doAfterDelete(Record record) {
        if (afterDeleteEvent != null)
            afterDeleteEvent.execute(record);
    }

    public void close() {
        if (this.head != null) {
            this.head.clear();
        }
        if (this.head_defs != null) {
            this.head_defs.clear();
        }
        fieldDefs.clear();
        records.clear();
        recNo = 0;
        fetchNo = -1;
    }

    public Record getHead() {
        if (head_defs == null) {
            head_defs = new FieldDefs();
        }
        if (head == null) {
            head = new Record(head_defs);
        }
        return head;
    }

    public final String getJSON() {
        StringBuilder builder = new StringBuilder();

        builder.append("{");

        if (this.state != 0)
            builder.append("\"state\":").append(this.state);

        if (this.message != null) {
            if (builder.length() > 1)
                builder.append(",");
            builder.append("\"message\":\"").append(this.message).append("\"");
        }

        if (head != null) {
            if (builder.length() > 1)
                builder.append(",");
            if (head.size() > 0)
                builder.append("\"head\":").append(head.toString());
        }
        if (this.size() > 0) {
            List<String> fields = this.getFieldDefs().getFields();
            Gson gson = new Gson();
            if (builder.length() > 1)
                builder.append(",");
            builder.append("\"dataset\":[").append(gson.toJson(fields));
            for (int i = 0; i < this.size(); i++) {
                Record record = this.getRecords().get(i);
                Map<String, Object> tmp1 = record.getItems();
                Map<String, Object> tmp2 = new LinkedHashMap<String, Object>();
                for (String field : fields) {
                    Object obj = tmp1.get(field);
                    if (obj == null) {
                        tmp2.put(field, "{}");
                    } else if (obj instanceof TDateTime) {
                        tmp2.put(field, obj.toString());
                    } else if (obj instanceof Date) {
                        tmp2.put(field, (new TDateTime((Date) obj)).toString());
                    } else {
                        tmp2.put(field, obj);
                    }
                }
                builder.append(",").append(gson.toJson(tmp2.values()));
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    public final DataSet setJSON(String json) {
        if (Utils.isEmpty(json)) {
            close();
            return this;
        }

        Gson gson = new GsonBuilder().serializeNulls().create();
        Map<String, Object> jsonmap = gson.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());

        if (jsonmap.containsKey("state")) {
            String value = String.valueOf(jsonmap.get("state"));
            this.setState(Integer.parseInt(value.split("\\.")[0]));
        }

        if (jsonmap.containsKey("message"))
            this.setMessage(String.valueOf(jsonmap.get("message")));

        if (jsonmap.containsKey("head"))
            this.getHead().setJSON(jsonmap.get("head"));

        if (jsonmap.containsKey("dataset")) {
            @SuppressWarnings("rawtypes")
            ArrayList dataset = (ArrayList) jsonmap.get("dataset");
            if (dataset != null && dataset.size() > 1) {
                @SuppressWarnings("rawtypes")
                ArrayList fields = (ArrayList) dataset.get(0);
                for (int i = 1; i < dataset.size(); i++) {
                    @SuppressWarnings("rawtypes")
                    ArrayList Recordj = (ArrayList) dataset.get(i);
                    Record record = this.append().getCurrent();
                    for (int j = 0; j < fields.size(); j++) {
                        Object obj = Recordj.get(j);
                        if (obj instanceof Double) {
                            double tmp = (double) obj;
                            if (tmp >= Integer.MIN_VALUE && tmp <= Integer.MAX_VALUE) {
                                Integer val = (int) tmp;
                                if (tmp == val) {
                                    obj = val;
                                }
                            }
                        }
                        record.setField(fields.get(j).toString(), obj);
                    }
                    this.post();
                }
                this.first();
            }
        }
        return this;
    }

    @Override
    public final String toString() {
        return getJSON();
    }

    public DataSet appendDataSet(DataSet source) {
        // 先复制字段定义
        FieldDefs tarDefs = this.getFieldDefs();
        for (String field : source.getFieldDefs().getFields()) {
            if (!tarDefs.exists(field)) {
                tarDefs.add(field);
            }
        }

        // 再复制所有数据
        for (int i = 0; i < source.records.size(); i++) {
            Record src_row = source.records.get(i);
            Record tar_row = this.append().getCurrent();
            for (String field : src_row.getFieldDefs().getFields()) {
                tar_row.setField(field, src_row.getField(field));
            }
            this.post();
        }
        return this;
    }

    /**
     * @param source      要复制的数据源
     * @param includeHead 是否连头部一起复制
     * @return 返回复制结果集
     */
    public DataSet appendDataSet(DataSet source, boolean includeHead) {
        this.appendDataSet(source);

        if (includeHead) {
            this.getHead().copyValues(source.getHead(), source.getHead().getFieldDefs());
        }

        return this;
    }

    // 支持对象序列化
    private void writeObject(ObjectOutputStream out) throws IOException {
        String json = this.getJSON();
        int strLen = json.length();
        out.writeInt(strLen);
        out.write(json.getBytes(StandardCharsets.UTF_8));
    }

    // 支持对象序列化
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        int strLen = in.readInt();
        byte[] strBytes = new byte[strLen];
        in.readFully(strBytes);
        String json = new String(strBytes, StandardCharsets.UTF_8);
        this.setJSON(json);
    }

    public boolean isReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    public int getState() {
        return state;
    }

    public DataSet setState(int state) {
        this.state = state;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public DataSet setMessage(String message) {
        this.message = message;
        return this;
    }

    protected final void setStorage(boolean storage) {
        this.storage = storage;
    }

    public final boolean isStorage() {
        return storage;
    }

    protected boolean isBatchSave() {
        return batchSave;
    }

    protected void setBatchSave(boolean batchSave) {
        this.batchSave = batchSave;
    }

    /**
     * 关闭写入存储设备功能
     */
    public final void disableStorage() {
        this.getFieldDefs().forEach(meta -> meta.setType(FieldType.Calculated));
        this.setStorage(false);
    }

    public static void main(String[] args) {
        DataSet dataSet = new DataSet();
        dataSet.onAppend((ds) -> {
            ds.setField("it", ds.size());
        });
        dataSet.onBeforePost((rs) -> {
            System.out.println("onBeforePost: " + rs.toString());
        });
        dataSet.onAfterPost((rs) -> {
            System.out.println("onAfterPost: " + rs.toString());
        });
        dataSet.onBeforeDelete((rs) -> {
            System.out.println("onBeforeDelete: " + rs.toString());
        });
        dataSet.onAfterDelete((rs) -> {
            System.out.println("onAfterDelete: " + rs.toString());
        });
        System.out.println(dataSet.getJSON());

        dataSet.setState(1);
        System.out.println(dataSet.getJSON());

        dataSet.setMessage("hello");
        System.out.println(dataSet.getJSON());

        dataSet.getHead().setField("token", "xxx");
        System.out.println(dataSet.getJSON());

        dataSet.append();
        dataSet.setField("code", "1");
        dataSet.setField("name", "a");
        dataSet.post();
        dataSet.append();
        dataSet.setField("code", "2");
        dataSet.setField("name", "b");
        dataSet.post();
        dataSet.delete();

        System.out.println(dataSet.getJSON());
        DataSet ds2 = new DataSet();
        ds2.setJSON(dataSet.toString());
    }

}
