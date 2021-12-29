package cn.cerc.db.nas;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.QueueOperator;

public class NasQuery extends DataSet implements IHandle {
    private static final long serialVersionUID = 8879520916623870766L;
    private static final Logger log = LoggerFactory.getLogger(NasQuery.class);
    // 文件目录
    private String filePath;
    // 文件名称
    private String fileName;
    private QueueOperator operator;
    private NasModel nasMode = NasModel.create;
    private SqlText sql = new SqlText(SqlServerType.Mysql);
    private boolean active;
    private ISession session;

    public NasQuery(IHandle handle) {
        super();
        this.session = handle.getSession();
    }

    public NasQuery open() {
        try {
            this.fileName = this.sql().text()
                    .substring(this.sql().text().indexOf("select") + 6, this.sql().text().indexOf("from")).trim();
            this.filePath = SqlText.findTableName(this.sql().text());
        } catch (Exception e) {
            throw new RuntimeException("command suggest: select fileName from filePath");
        }
        // 校验数据
        if (Utils.isEmpty(this.filePath)) {
            throw new RuntimeException("please enter the file path");
        }
        if (nasMode == NasModel.readWrite) {
            File file = FileUtils.getFile(this.filePath, this.fileName);
            try {
                String json = FileUtils.readFileToString(file, StandardCharsets.UTF_8.name());
                this.setJson(json);
                this.setActive(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    // 删除文件或目录
    @Override
    protected final void deleteStorage(DataRow record) {
        File file = FileUtils.getFile(this.filePath, this.fileName);
        FileUtils.deleteQuietly(file);
        log.info("文件:" + file.getPath() + "删除成功");
    }

    public void save() {
        File file = FileUtils.getFile(this.filePath, this.fileName);
        try {
            String content = this.json();
            FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8.name(), false);// 不存在则创建,存在则不追加到文件末尾
        } catch (IOException e) {
            log.info("文件:" + file.getPath() + "保存失败");
            e.printStackTrace();
        }
        log.info("文件:" + file.getPath() + "保存成功");
    }

    public QueueOperator getOperator() {
        if (operator == null) {
            operator = new QueueOperator();
        }
        return operator;
    }

    public NasModel getNasMode() {
        return nasMode;
    }

    public void setNasMode(NasModel nasMode) {
        this.nasMode = nasMode;
    }

    public NasQuery add(String sqlText) {
        this.sql.add(sqlText);
        return this;
    }

    public NasQuery add(String format, Object... args) {
        sql.add(format, args);
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

    public boolean getActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public SqlText sql() {
        return sql;
    }

    @Deprecated
    public final SqlText getSqlText() {
        return sql();
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public NasQuery setJson(String json) {
        super.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
