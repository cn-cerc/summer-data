package cn.cerc.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.core.FieldMeta.FieldKind;

public class DataSet implements Serializable, DataSource, Iterable<DataRow>, IRecord {
    private static final Logger log = LoggerFactory.getLogger(DataSet.class);
    private static final long serialVersionUID = 873159747066855363L;
    private static final ClassResource res = new ClassResource(DataSet.class, SummerCore.ID);
    private int recNo = 0;
    private int fetchNo = -1;
    private int state = 0;
    private String message = null;
    private List<DataSetInsertEvent> appendListener;
    private List<DataSetBeforeUpdateEvent> beforePostListener;
    private List<DataSetAfterUpdateEvent> afterPostListener;
    private List<DataSetBeforeDeleteEvent> beforeDeleteListener;
    private List<DataSetAfterDeleteEvent> afterDeleteListener;

    private boolean readonly;
    // 在变更时，是否需要同步保存到数据库中
    private boolean storage;
    // 批次保存模式，默认为post与delete立即保存
    private boolean batchSave = false;
    // 仅当batchSave为true时，garbage才有记录存在
    private List<DataRow> garbage = new ArrayList<>();
    // 搜索加速器
    private SearchDataSet search;
    // gson时，是否输出meta讯息
    private boolean metaInfo;
    // 头部记录
    private DataRow head = new DataRow();
    // 明细记录
    private List<DataRow> records = new ArrayList<>();
    // 明细字段定义
    private FieldDefs fields = new FieldDefs();
    // 是否进入CURD模式
    private boolean curd;

    public DataSet() {
        super();
    }

    public DataSet append() {
        DataRow record = new DataRow(this).setState(DataRowState.Insert);
        records.add(record);
        recNo = records.size();
        doAppend(record);
        return this;
    }

    @Deprecated
    public DataSet append(int index) {
        DataRow record = new DataRow(this).setState(DataRowState.Insert);
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

    public DataSet edit() {
        if (bof() || eof())
            throw new RuntimeException(res.getString(1, "当前记录为空，无法修改"));
        DataRow row = this.current();
        if (row.state() == DataRowState.None) {
            row.setHistory(row.clone());
            row.setState(DataRowState.Update);
        }
        return this;
    }

    public DataSet delete() {
        if (bof() || eof())
            throw new RuntimeException(res.getString(2, "当前记录为空，无法删除"));
        DataRow record = this.current();
        if (search != null)
            search.remove(record);
        records.remove(recNo - 1);
        if (fetchNo > -1) {
            fetchNo--;
        }
        if (record.state() == DataRowState.Insert)
            return this;

        if (record.state() == DataRowState.Update)
            garbage.add(record.history().setState(DataRowState.Delete));
        else
            garbage.add(record.setState(DataRowState.Delete));

        if (!this.isBatchSave()) {
            doBeforeDelete(record);
            if (this.isStorage()) {
                try {
                    deleteStorage(record);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterDelete(record);
        }
        return this;
    }

    public final void post() {
        if (this.isBatchSave())
            return;
        DataRow dataRow = this.current();
        if (dataRow.state() == DataRowState.Insert) {
            doBeforePost(dataRow);
            if (this.isStorage()) {
                try {
                    insertStorage(dataRow);
                } catch (Exception e) {
                    if (e.getMessage().contains("Data too long"))
                        log.error(dataRow.toString());
                    throw new RuntimeException(e);
                }
            }
            doAfterPost(dataRow);
        } else if (dataRow.state() == DataRowState.Update) {
            doBeforePost(dataRow);
            if (this.isStorage()) {
                try {
                    updateStorage(dataRow);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterPost(dataRow);
        }
    }

    protected void insertStorage(DataRow record) throws Exception {

    }

    protected void updateStorage(DataRow record) throws Exception {

    }

    protected void deleteStorage(DataRow record) throws Exception {

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

    @Override
    @Deprecated
    public DataRow getCurrent() {
        return current();
    }

    @Override
    public DataRow current() {
        if (this.eof()) {
            throw new RuntimeException(String.format("[%s]eof == true", this.getClass().getName()));
        } else if (this.bof()) {
            throw new RuntimeException(String.format("[%s]bof == true", this.getClass().getName()));
        } else {
            return records.get(recNo - 1);
        }
    }

    /**
     * 使用此方法只允许取数据，不允许进行变更
     *
     * @return List
     */
    public List<DataRow> records() {
        return records;
    }

    @Deprecated
    public List<DataRow> getRecords() {
        return records();
    }

    public int recNo() {
        return recNo;
    }

    @Deprecated
    public int getRecNo() {
        return recNo();
    }

    public DataSet setRecNo(int recNo) {
        if (recNo > this.records.size()) {
            String msg = String.format(res.getString(3, "[%s]RecNo %d 大于总长度 %d"), this.getClass().getName(), recNo,
                    this.records.size());
            throw new RuntimeException(msg);
        } else {
            this.recNo = recNo;
        }
        return this;
    }

    public int size() {
        return this.records.size();
    }

    public FieldDefs fields() {
        return this.fields;
    }

    @Deprecated
    public FieldDefs getFieldDefs() {
        return fields();
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
            if (this.current().equalsValues(fieldValueMap)) {
                return true;
            }
        }
        return false;
    }

    // 用于查找多次，调用时，会先进行排序，以方便后续的相同Key查找
    public boolean locate(String fields, Object... values) {
        if (search == null)
            search = new SearchDataSet(this);

        DataRow record = search.get(fields, values);
        if (record != null) {
            this.setRecNo(this.records.indexOf(record) + 1);
            return true;
        } else {
            return false;
        }
    }

    public DataRow lookup(String fields, Object... values) {
        if (search == null)
            search = new SearchDataSet(this);

        return search.get(fields, values);
    }

    @Override
    public Object getValue(String field) {
        return this.current().getValue(field);
    }

    // 排序
    public void setSort(String... fields) {
        Collections.sort(this.getRecords(), new RecordComparator(fields));
    }

    public void setSort(Comparator<DataRow> func) {
        Collections.sort(this.getRecords(), func);
    }

    @Deprecated
    public TDate getDate(String field) {
        return this.current().getDate(field);
    }

    @Deprecated
    public TDateTime getDateTime(String field) {
        return this.current().getDateTime(field);
    }

    @Override
    public DataSet setValue(String field, Object value) {
        this.current().setValue(field, value);
        return this;
    }

    public DataSet setNull(String field) {
        return setValue(field, null);
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

    public void copyRecord(DataRow source, FieldDefs defs) {
        DataRow record = this.current();
        if (search != null) {
            search.remove(record);
            record.copyValues(source, defs);
            search.append(record);
        } else {
            record.copyValues(source, defs);
        }
    }

    public void copyRecord(DataRow source, String... fields) {
        DataRow record = this.current();
        if (search != null) {
            search.remove(record);
            record.copyValues(source, fields);
            search.append(record);
        } else {
            record.copyValues(source, fields);
        }
    }

    public void copyRecord(DataRow sourceRecord, String[] sourceFields, String[] targetFields) {
        if (targetFields.length != sourceFields.length)
            throw new RuntimeException(res.getString(7, "前后字段数目不一样，请您确认！"));
        DataRow record = this.current();
        if (search != null) {
            search.remove(record);
            for (int i = 0; i < sourceFields.length; i++)
                record.setValue(targetFields[i], sourceRecord.getValue(sourceFields[i]));
            search.append(record);
        } else {
            for (int i = 0; i < sourceFields.length; i++)
                record.setValue(targetFields[i], sourceRecord.getValue(sourceFields[i]));
        }
    }

    public boolean isNull(String field) {
        Object obj = current().getValue(field);
        return obj == null || "".equals(obj);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return records.iterator();
    }

    @Override
    public boolean exists(String field) {
        return this.fields().exists(field);
    }

    public interface DataSetInsertEvent {
        void insertRecord(DataSet record);
    }

    // 用于设置字段的初始值，如 pid, it 等
    public final void onAppend(DataSetInsertEvent appendEvent) {
        if (this.appendListener == null)
            this.appendListener = new ArrayList<>();
        this.appendListener.add(appendEvent);
    }

    public final List<DataSetInsertEvent> getOnAppend() {
        if (this.appendListener == null)
            this.appendListener = new ArrayList<>();
        return appendListener;
    }

    protected final void doAppend(DataRow record) {
        if (appendListener != null)
            appendListener.forEach(event -> event.insertRecord(this));
        if (search != null)
            search.append(record);
    }

    public interface DataSetBeforeUpdateEvent {
        void updateRecordBefore(DataRow record);
    }

    // 用于在保存之前检查字段值是否合法，若失败则抛异常，以及给updateTime赋值
    public final void onBeforePost(DataSetBeforeUpdateEvent beforePostEvent) {
        if (this.beforePostListener == null)
            this.beforePostListener = new ArrayList<>();
        this.beforePostListener.add(beforePostEvent);
    }

    public final List<DataSetBeforeUpdateEvent> getBeforePost() {
        if (this.beforePostListener == null)
            this.beforePostListener = new ArrayList<>();
        return beforePostListener;
    }

    protected final void doBeforePost(DataRow record) {
        if (beforePostListener != null)
            beforePostListener.forEach(event -> event.updateRecordBefore(record));
    }

    public interface DataSetAfterUpdateEvent {
        void updateRecordAfter(DataRow record);
    }

    // 用于在保存成功之后进行记录
    public final void onAfterPost(DataSetAfterUpdateEvent afterPostEvent) {
        if (this.afterPostListener == null)
            this.afterPostListener = new ArrayList<>();
        this.afterPostListener.add(afterPostEvent);
    }

    public final List<DataSetAfterUpdateEvent> getAfterPost() {
        if (this.afterPostListener == null)
            this.afterPostListener = new ArrayList<>();
        return afterPostListener;
    }

    protected final void doAfterPost(DataRow record) {
        if (afterPostListener != null)
            afterPostListener.forEach(event -> event.updateRecordAfter(record));
        record.setState(DataRowState.None);
    }

    public interface DataSetBeforeDeleteEvent {
        void deleteRecordBefore(DataRow record);
    }

    // 用于在实际删除前检查是否允许删除
    public final void onBeforeDelete(DataSetBeforeDeleteEvent beforeDeleteEvent) {
        if (this.beforeDeleteListener == null)
            this.beforeDeleteListener = new ArrayList<>();
        this.beforeDeleteListener.add(beforeDeleteEvent);
    }

    public final List<DataSetBeforeDeleteEvent> getBeforeDelete() {
        if (this.beforeDeleteListener == null)
            this.beforeDeleteListener = new ArrayList<>();
        return beforeDeleteListener;
    }

    protected final void doBeforeDelete(DataRow record) {
        if (beforeDeleteListener != null)
            beforeDeleteListener.forEach(event -> event.deleteRecordBefore(record));
    }

    public interface DataSetAfterDeleteEvent {
        void deleteRecordAfter(DataRow record);
    }

    // 用于在删除成功后进行记录
    public final void onAfterDelete(DataSetAfterDeleteEvent afterDeleteEvent) {
        if (this.afterDeleteListener == null)
            this.afterDeleteListener = new ArrayList<>();
        this.afterDeleteListener.add(afterDeleteEvent);
    }

    public final List<DataSetAfterDeleteEvent> getAfterDelete() {
        if (this.afterDeleteListener == null)
            this.afterDeleteListener = new ArrayList<>();
        return afterDeleteListener;
    }

    protected final void doAfterDelete(DataRow record) {
        if (afterDeleteListener != null)
            afterDeleteListener.forEach(event -> event.deleteRecordAfter(record));
    }

    public void emptyDataSet() {
        garbage.clear();
        records.clear();
        recNo = 0;
        fetchNo = -1;
    }

    public void clear() {
        emptyDataSet();
        fields.clear();
        head.clear();
        if (search != null) {
            search.clear();
            search = null;
        }
    }

    @Deprecated
    public void close() {
        clear();
    }

    public final DataRow head() {
        return head;
    }

    @Deprecated
    public final DataRow getHead() {
        return head();
    }

    @Override
    public final String toString() {
        return toJson();
    }

    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Deprecated
    public String toJson() {
        return json();
    }

    public DataSet setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        this.first();
        return this;
    }

    @Deprecated
    public DataSet fromJson(String json) {
        return setJson(json);
    }

    /**
     * @param source      要复制的数据源
     * @param includeHead 是否连头部一起复制
     * @return 返回复制结果集
     */
    public DataSet appendDataSet(DataSet source, boolean includeHead) {
        this.appendDataSet(source);
        if (includeHead)
            this.head().copyValues(source.head(), source.head().fields());
        return this;
    }

    public DataSet appendDataSet(DataSet source) {
        if (search != null) {
            search.clear();
            search = null;
        }
        // 先复制字段定义
        FieldDefs tarDefs = this.fields();
        for (String field : source.fields().names()) {
            if (!tarDefs.exists(field)) {
                tarDefs.add(field);
            }
        }

        // 再复制所有数据
        for (int i = 0; i < source.records.size(); i++) {
            DataRow src_row = source.records.get(i);
            DataRow tar_row = this.append().current();
            for (String field : src_row.fields().names()) {
                tar_row.setValue(field, src_row.getValue(field));
            }
            this.post();
        }
        return this;
    }

    @Override
    public boolean isReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    public int state() {
        return state;
    }

    @Deprecated
    public int getState() {
        return state();
    }

    public DataSet setState(int state) {
        this.state = state;
        return this;
    }

    public String message() {
        return message;
    }

    @Deprecated
    public String getMessage() {
        return message();
    }

    public DataSet setMessage(String message) {
        this.message = message;
        return this;
    }

    public boolean storage() {
        return storage;
    }

    protected final void setStorage(boolean storage) {
        this.storage = storage;
    }

    @Deprecated
    public final boolean isStorage() {
        return storage();
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
    public DataSet disableStorage() {
        this.fields().forEach(meta -> {
            if (meta.getKind() == FieldKind.Storage)
                meta.setKind(FieldKind.Memory);
        });
        this.setStorage(false);
        return this;
    }

    public SearchDataSet search() {
        return search;
    }

    @Deprecated
    public SearchDataSet getSearch() {
        return search;
    }

    public boolean metaInfo() {
        return metaInfo;
    }

    @Deprecated
    public final boolean isMetaInfo() {
        return metaInfo();
    }

    public final DataSet setMetaInfo(boolean metaInfo) {
        this.metaInfo = metaInfo;
        return this;
    }

    public final DataSet buildMeta() {
        if (head.fields().size() > 0) {
            head.fields().forEach(def -> def.getFieldType().put(head.getValue(def.getCode())));
        }
        records.forEach(row -> {
            this.fields().forEach(def -> def.getFieldType().put(row.getValue(def.getCode())));
        });
        return this;
    }

    public List<DataRow> garbage() {
        return garbage;
    }

    @Deprecated
    protected final List<DataRow> getDelList() {
        return garbage();
    }

    public boolean curd() {
        return curd;
    }

    public DataSet setCurd(boolean value) {
        curd = value;
        return this;
    }

    public DataSet mergeChangeLog() {
        for (int i = 0; i < this.size(); i++) {
            DataRow row = records.get(i);
            row.setState(DataRowState.None);
        }
        garbage.clear();
        this.recNo = 0;
        return this;
    }

    public static void main(String[] args) {
        DataSet ds1 = new DataSet();
        ds1.onAppend((ds) -> {
            ds.setValue("it", ds.size());
        });
        ds1.onBeforePost((rs) -> {
            System.out.println("onBeforePost: " + rs.toString());
        });
        ds1.onAfterPost((rs) -> {
            System.out.println("onAfterPost: " + rs.toString());
        });
        ds1.onBeforeDelete((rs) -> {
            System.out.println("onBeforeDelete: " + rs.toString());
        });
        ds1.onAfterDelete((rs) -> {
            System.out.println("onAfterDelete: " + rs.toString());
        });
        System.out.println(ds1.toJson());

        ds1.setState(1);
        System.out.println(ds1.toJson());

        ds1.setMessage("hello");
        System.out.println(ds1.toJson());

        ds1.head().setValue("token", "xxx");
        System.out.println(ds1.toJson());

        ds1.append();
        ds1.setValue("code", "1");
        ds1.setValue("name", "a");
        ds1.setValue("value", 10);

        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        ds1.setValue("code", list);
//
//        DataSet ds0 = new DataSet();
//        ds0.append();
//        ds0.setField("partCode_", "p001");
//        ds1.setField("name", ds0);

        ds1.post();
        ds1.append();
        ds1.setValue("code", "2");
        ds1.setValue("name", "b");
        ds1.post();
//        ds1.delete();

        ds1.setState(1);
        ds1.setMessage("test");

        ds1.head().setValue("title", "test");
        ds1.head().setValue("tbDate", new FastDate());
        ds1.head().setValue("appDate", new Date());
        ds1.head().setValue("user", null);

        ds1.fields().add("it").setName("序").setRemark("自动赋值").setType(Integer.class);
        ds1.fields().add("code").setName("编号").setType(String.class, 30);
        ds1.fields().add("name").setName("名称").setType(String.class, 30);
        ds1.head().fields().add("title").setName("标题").setType(String.class, 30);

        ds1.buildMeta().setMetaInfo(true);
        System.out.println(ds1.json());

        DataSet ds2 = new DataSet().setJson(ds1.json());
        System.out.println(ds2.json());

        DataSet ds3 = new DataSet();
        ds3.head().fields().add("title").setName("标题").setType(String.class, 30);
        ds3.fields().add("it").setName("序").setRemark("自动增加").setType(Integer.class);
        ds3.fields().add("code").setName("编号").setType(String.class, 30);
        ds3.fields().add("name").setName("名称").setType(String.class, 30);
        ds3.setMetaInfo(true);

        System.out.println(ds3.json());
        System.out.println(ds3.setMetaInfo(false).json());

        ds3.fields().remove("it");
        ds3.append().setValue("code", "a01");
        ds3.append().setValue("code", "a02");
        ds3.mergeChangeLog();
        System.out.println(ds3.json());
        ds3.first();
        ds3.delete();
        ds3.edit().setValue("code", "a02-new");
        ds3.append().setValue("code", "a03");
        System.out.println(ds3.setCurd(true));
        System.out.println(ds3.setCurd(false));
        System.out.println(ds3.setCurd(true).setMetaInfo(true));
        System.out.println(ds3.setCurd(false).setMetaInfo(true));

        DataSet ds4 = new DataSet();
        ds4.setJson(
                "{\"meta\":{\"head\":[{\"title\":[\"标题\",\"s30\"]}],\"body\":[{\"code\":[\"编号\",\"s30\"]},{\"name\":[\"名称\",\"s30\"]},{\"_state_\":[]}]},\"head\":[null],\"body\":[[\"a02\",null,4],[\"a02-new\",null,2],[\"a03\",null,1],[\"a01\",null,3]]}\r\n"
                        + "");
        System.out.println(ds4);
        System.out.println(ds4.setCurd(false));
        System.out.println(ds4.setMetaInfo(false));
    }

}
