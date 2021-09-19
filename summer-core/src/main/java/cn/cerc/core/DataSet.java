package cn.cerc.core;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
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

public class DataSet implements Serializable, DataSource, Iterable<DataRow> {
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
    // 仅当batchSave为true时，delList才有记录存在
    private List<DataRow> delList = new ArrayList<>();
    // 搜索加速器
    private SearchDataSet search;
    // gson时，是否输出meta讯息
    private boolean metaInfo;
    // 头部记录
    private DataRow head = new DataRow();
    // 明细记录
    private List<DataRow> records = new ArrayList<>();
    // 明细字段定义
    private FieldDefs fieldDefs = new FieldDefs();

    public DataSet() {
        super();
    }

    public DataSet append() {
        DataRow record = new DataRow(this).setState(RecordState.dsInsert);
        this.records.add(record);
        recNo = records.size();
        doAppend(record);
        return this;
    }

    @Deprecated
    public DataSet append(int index) {
        DataRow record = new DataRow(this).setState(RecordState.dsInsert);
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
        if (bof() || eof())
            throw new RuntimeException(res.getString(1, "当前记录为空，无法修改"));
        this.getCurrent().setState(RecordState.dsEdit);
    }

    public final void delete() {
        if (bof() || eof())
            throw new RuntimeException(res.getString(2, "当前记录为空，无法删除"));
        DataRow record = this.getCurrent();
        if (search != null)
            search.remove(record);
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
                    log.error(e.getMessage());
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterDelete(record);
        }
    }

    public final void post() {
        if (this.isBatchSave())
            return;
        DataRow record = this.getCurrent();
        if (record.getState() == RecordState.dsInsert) {
            doBeforePost(record);
            if (this.isStorage()) {
                try {
                    insertStorage(record);
                } catch (Exception e) {
                    log.error(e.getMessage());
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
                    log.error(e.getMessage());
                    throw new RuntimeException(e.getMessage());
                }
            }
            doAfterPost(record);
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
    public DataRow getCurrent() {
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
    public List<DataRow> getRecords() {
        return records;
    }

//    @Deprecated
//    public DataRow getIndex(int index) {
//        return this.setRecNo(index + 1).getCurrent();
//    }

    public int getRecNo() {
        return recNo;
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

    public Object getField(String field) {
        return this.getCurrent().getField(field);
    }

    // 排序
    public void setSort(String... fields) {
        Collections.sort(this.getRecords(), new RecordComparator(fields));
    }

    public void setSort(Comparator<DataRow> func) {
        Collections.sort(this.getRecords(), func);
    }

    public String getString(String field) {
        return this.getCurrent().getString(field);
    }

    public double getDouble(String field) {
        return this.getCurrent().getDouble(field);
    }

    public boolean getBoolean(String field) {
        return this.getCurrent().getBoolean(field);
    }

    public int getInt(String field) {
        return this.getCurrent().getInt(field);
    }

    public long getLong(String field) {
        return this.getCurrent().getLong(field);
    }

    public BigInteger getBigInteger(String field) {
        return this.getCurrent().getBigInteger(field);
    }

    public BigDecimal getBigDecimal(String field) {
        return this.getCurrent().getBigDecimal(field);
    }

    public Datetime getDatetime(String field) {
        return this.getCurrent().getDatetime(field);
    }

    public FastDate getFastDate(String field) {
        return this.getCurrent().getDatetime(field).toFastDate();
    }

    public FastTime getFastTime(String field) {
        return this.getCurrent().getDatetime(field).toFastTime();
    }

    @Deprecated
    public TDate getDate(String field) {
        return this.getCurrent().getDate(field);
    }

    @Deprecated
    public TDateTime getDateTime(String field) {
        return this.getCurrent().getDateTime(field);
    }

    public DataSet setField(String field, Object value) {
        if (field == null || "".equals(field))
            throw new RuntimeException("field is null!");
        this.getCurrent().setField(field, value);
        return this;
    }

    public DataSet setNull(String field) {
        return setField(field, null);
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
        DataRow record = this.getCurrent();
        if (search != null) {
            search.remove(record);
            record.copyValues(source, defs);
            search.append(record);
        } else {
            record.copyValues(source, defs);
        }
    }

    public void copyRecord(DataRow source, String... fields) {
        DataRow record = this.getCurrent();
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
        DataRow record = this.getCurrent();
        if (search != null) {
            search.remove(record);
            for (int i = 0; i < sourceFields.length; i++)
                record.setField(targetFields[i], sourceRecord.getField(sourceFields[i]));
            search.append(record);
        } else {
            for (int i = 0; i < sourceFields.length; i++)
                record.setField(targetFields[i], sourceRecord.getField(sourceFields[i]));
        }
    }

    public boolean isNull(String field) {
        Object obj = getCurrent().getField(field);
        return obj == null || "".equals(obj);
    }

    @Override
    public Iterator<DataRow> iterator() {
        return records.iterator();
    }

    public boolean exists(String field) {
        return this.getFieldDefs().exists(field);
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
        record.setState(RecordState.dsNone);
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

    public void close() {
        this.head.clear();
        if (search != null) {
            search.clear();
            search = null;
        }
        fieldDefs.clear();
        records.clear();
        recNo = 0;
        fetchNo = -1;
    }

    public DataRow getHead() {
        return head;
    }

    @Override
    public final String toString() {
        return toJson();
    }

    public String toJson() {
        return new DataSetGson<>(this).encode();
    }

    public DataSet fromJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
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

    public DataSet appendDataSet(DataSet source) {
        if (search != null) {
            search.clear();
            search = null;
        }
        // 先复制字段定义
        FieldDefs tarDefs = this.getFieldDefs();
        for (String field : source.getFieldDefs().getFields()) {
            if (!tarDefs.exists(field)) {
                tarDefs.add(field);
            }
        }

        // 再复制所有数据
        for (int i = 0; i < source.records.size(); i++) {
            DataRow src_row = source.records.get(i);
            DataRow tar_row = this.append().getCurrent();
            for (String field : src_row.getFieldDefs().getFields()) {
                tar_row.setField(field, src_row.getField(field));
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
        this.getFieldDefs().forEach(meta -> {
            if (meta.getKind() == FieldKind.Storage)
                meta.setKind(FieldKind.Memory);
        });
        this.setStorage(false);
    }

    public SearchDataSet getSearch() {
        return search;
    }

    public final boolean isMetaInfo() {
        return metaInfo;
    }

    public final DataSet setMetaInfo(boolean metaInfo) {
        this.metaInfo = metaInfo;
        return this;
    }

    public final DataSet buildMeta() {
        if (head.getFieldDefs().size() > 0) {
            head.getFieldDefs().forEach(def -> def.getFieldType().put(head.getField(def.getCode())));
        }
        records.forEach(row -> {
            this.getFieldDefs().forEach(def -> def.getFieldType().put(row.getField(def.getCode())));
        });
        return this;
    }

    protected final List<DataRow> getDelList() {
        return delList;
    }

    public static void main(String[] args) {
        DataSet ds1 = new DataSet();
        ds1.onAppend((ds) -> {
            ds.setField("it", ds.size());
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

        ds1.getHead().setField("token", "xxx");
        System.out.println(ds1.toJson());

        ds1.append();
        ds1.setField("code", "1");
        ds1.setField("name", "a");
        ds1.setField("value", 10);

        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        ds1.setField("code", list);
//
//        DataSet ds0 = new DataSet();
//        ds0.append();
//        ds0.setField("partCode_", "p001");
//        ds1.setField("name", ds0);

        ds1.post();
        ds1.append();
        ds1.setField("code", "2");
        ds1.setField("name", "b");
        ds1.post();
//        ds1.delete();

        ds1.setState(1);
        ds1.setMessage("test");

        ds1.getHead().setField("title", "test");
        ds1.getHead().setField("tbDate", new FastDate());
        ds1.getHead().setField("appDate", new Date());
        ds1.getHead().setField("user", null);

        ds1.getFieldDefs().add("it").setName("序").setRemark("自动赋值").setType(Integer.class);
        ds1.getFieldDefs().add("code").setName("编号").setType(String.class, 30);
        ds1.getFieldDefs().add("name").setName("名称").setType(String.class, 30);
        ds1.getHead().getFieldDefs().add("title").setName("标题").setType(String.class, 30);

        ds1.buildMeta().setMetaInfo(true);
        System.out.println(ds1.toJson());

        DataSet ds2 = new DataSet().fromJson(ds1.toJson());
        System.out.println(ds2.toJson());

        DataSet ds3 = new DataSet();
        ds3.getHead().getFieldDefs().add("title").setName("标题").setType(String.class, 30);
        ds3.getFieldDefs().add("it").setName("序").setRemark("自动增加").setType(Integer.class);
        ds3.getFieldDefs().add("code").setName("编号").setType(String.class, 30);
        ds3.getFieldDefs().add("name").setName("名称").setType(String.class, 30);
        ds3.setMetaInfo(true);

        System.out.println(ds3.toJson());

        System.out.println(new DataSet().fromJson("{}"));
    }

}
