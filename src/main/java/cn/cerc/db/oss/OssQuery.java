package cn.cerc.db.oss;

import java.io.ByteArrayInputStream;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.OssOperator;

@Deprecated
public class OssQuery extends DataSet implements IHandle {
    private static final long serialVersionUID = 3346060985794794816L;
    private OssConnection connection;
    private OssOperator operator;
    // 文件名称
    private String fileName;
    private OssMode ossMode = OssMode.create;
    private ISession session;
    private SqlText sql = new SqlText(SqlServerType.Mysql);
    private boolean active;

    public OssQuery(IHandle handle) {
        super();
        this.session = handle.getSession();
        connection = (OssConnection) getSession().getProperty(OssConnection.sessionId);
    }

    public OssQuery open() {
        try {
            this.fileName = SqlText.findTableName(this.sql().text());
            if (ossMode == OssMode.readWrite) {
                String value = connection.getContent(this.fileName);
                if (value != null) {
                    this.setJson(value);
                    this.setActive(true);
                }
            }
            return this;
        } catch (Exception e) {
            throw new RuntimeException("command suggest: select * from objectId");
        }
    }

    /**
     * 删除文件
     */
    public void remove() {
        connection.delete(this.fileName);
    }

    public void save() {
        String content = this.json();
        connection.upload(fileName, new ByteArrayInputStream(content.getBytes()));
    }

    public OssOperator getOperator() {
        if (operator == null) {
            operator = new OssOperator();
        }
        return operator;
    }

    public OssMode getOssMode() {
        return ossMode;
    }

    public void setOssMode(OssMode ossMode) {
        this.ossMode = ossMode;
    }

    public OssQuery add(String sqlText) {
        this.sql.add(sqlText);
        return this;
    }

    public OssQuery add(String format, Object... args) {
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

    public boolean isActive() {
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
        return sql;
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public OssQuery setJson(String json) {
        super.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }
}
