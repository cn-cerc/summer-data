package cn.cerc.db.oss;

import java.io.ByteArrayInputStream;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.OssOperator;

public class OssQuery extends DataSet implements IHandle {
    private static final long serialVersionUID = 3346060985794794816L;
    private OssConnection connection;
    private OssOperator operator;
    // 文件名称
    private String fileName;
    private OssMode ossMode = OssMode.create;
    private ISession session;
    private SqlText sqlText = new SqlText();
    private boolean active;

    public OssQuery(IHandle handle) {
        super();
        this.session = handle.getSession();
        connection = (OssConnection) getSession().getProperty(OssConnection.sessionId);
    }

    public OssQuery open() {
        try {
            this.fileName = SqlText.findTableName(this.getSqlText().getText());
            if (ossMode == OssMode.readWrite) {
                String value = connection.getContent(this.fileName);
                if (value != null) {
                    this.fromJson(value);
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
        String content = this.toJson();
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

    public OssQuery add(String sql) {
        sqlText.add(sql);
        return this;
    }

    public OssQuery add(String format, Object... args) {
        sqlText.add(format, args);
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

    public SqlText getSqlText() {
        return sqlText;
    }

    @Override
    public String toJson() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public OssQuery fromJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }
}
