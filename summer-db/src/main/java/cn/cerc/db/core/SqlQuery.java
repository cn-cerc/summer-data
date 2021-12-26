package cn.cerc.db.core;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.core.DataRow;
import cn.cerc.core.DataRowState;
import cn.cerc.core.DataSet;
import cn.cerc.core.DataSetGson;
import cn.cerc.core.FieldDefs;
import cn.cerc.core.FieldMeta.FieldKind;
import cn.cerc.core.ISession;
import cn.cerc.core.SqlServerType;
import cn.cerc.core.SqlServerTypeException;
import cn.cerc.core.SqlText;
import cn.cerc.core.Utils;
import cn.cerc.db.mssql.MssqlServer;
import cn.cerc.db.mysql.MysqlServer;
import cn.cerc.db.mysql.MysqlServerMaster;
import cn.cerc.db.mysql.MysqlServerSlave;
import cn.cerc.db.sqlite.SqliteServer;

public class SqlQuery extends DataSet implements IHandle {
    private static final long serialVersionUID = -6671201813972797639L;
    private static final Logger log = LoggerFactory.getLogger(SqlQuery.class);
    // 数据集是否有打开
    private boolean active = false;
    // 若数据有取完，则为true，否则为false
    private boolean fetchFinish;
    // 数据库保存操作执行对象
    private SqlOperator operator;
    // SqlCommand 指令
    private SqlText sql;
    // 运行环境
    private ISession session;
    private SqlServerType sqlServerType;
    //
    private ISqlServer server;
    private ISqlServer master;
    private ISqlServer salve;

    public SqlQuery(IHandle handle, SqlServerType sqlServerType) {
        super();
        this.sqlServerType = sqlServerType;
        this.sql = new SqlText(sqlServerType);
        if (handle != null)
            this.session = handle.getSession();
    }

    @Override
    public final ISession getSession() {
        return session;
    }

    @Override
    public final void setSession(ISession session) {
        this.session = session;
    }

    @Override
    public final void clear() {
        this.setActive(false);
        this.operator = null;
        this.sql().clear();
        super.clear();
    }

    public SqlQuery open() {
        open(true);
        return this;
    }

    public final SqlQuery openReadonly() {
        open(false);
        return this;
    }

    private final void open(boolean masterServer) {
        this.setStorage(masterServer);
        this.setFetchFinish(true);
        String sql = sql().getCommand();
        log.debug(sql.replaceAll("\r\n", " "));
        try (ConnectionClient client = getConnectionClient()) {
            try (Statement st = client.getConnection().createStatement()) {
                try (ResultSet rs = st.executeQuery(sql.replace("\\", "\\\\"))) {
                    // 取出所有数据
                    append(rs);
                    this.first();
                    this.setActive(true);
                }
            }
        } catch (Exception e) {
            log.error(sql);
            throw new RuntimeException(e);
        }
    }

    // 追加相同数据表的其它记录，与已有记录合并
    public int attach(String sqlText) {
        if (!this.active()) {
            this.clear();
            this.add(sqlText);
            this.open();
            return this.size();
        }

        log.debug(sqlText.replaceAll("\r\n", " "));
        try (ConnectionClient client = getConnectionClient()) {
            try (Statement st = client.getConnection().createStatement()) {
                try (ResultSet rs = st.executeQuery(sqlText.replace("\\", "\\\\"))) {
                    int oldSize = this.size();
                    append(rs);
                    return this.size() - oldSize;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public final void save() {
        if (!this.isBatchSave())
            throw new RuntimeException("batchSave is false");
        ConnectionClient client = null;
        try {
            if (this.storage())
                client = getConnectionClient();

            // 先执行删除
            for (DataRow record : garbage()) {
                doBeforeDelete(record);
                if (this.storage())
                    operator().delete(client.getConnection(), record);
                doAfterDelete(record);
            }
            // 再执行增加、修改
            this.first();
            while (this.fetch()) {
                DataRow record = this.current();
                if (record.state().equals(DataRowState.Insert)) {
                    doBeforePost(record);
                    if (this.storage())
                        operator().insert(client.getConnection(), record);
                    doAfterPost(record);
                    record.setState(DataRowState.None);
                } else if (record.state().equals(DataRowState.Update)) {
                    doBeforePost(record);
                    if (this.storage())
                        operator().update(client.getConnection(), record);
                    doAfterPost(record);
                    record.setState(DataRowState.None);
                }
            }
            garbage().clear();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                client = null;
            }
        }
    }

    private void append(ResultSet rs) throws SQLException {
        // 取得字段清单
        ResultSetMetaData meta = rs.getMetaData();
        FieldDefs defs = this.fields();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String field = meta.getColumnLabel(i);
            if (!defs.exists(field))
                defs.add(field, FieldKind.Storage);
        }
        // 取得所有数据
        int total = this.size();
        while (rs.next()) {
            total++;
            if (this.maximum() > -1 && this.maximum() < total) {
                setFetchFinish(false);
                break;
            }
            DataRow record = new DataRow(this).setState(DataRowState.Insert);
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String fn = rs.getMetaData().getColumnLabel(i);
                record.setValue(fn, rs.getObject(fn));
            }
            record.setState(DataRowState.None);
            this.records().add(record);
            this.last();
        }
        if (this.maximum() > -1) {
            BigdataException.check(this, this.size());
        }
    }

    @Override
    public final void insertStorage(DataRow record) throws Exception {
        try (ConnectionClient client = getConnectionClient()) {
            if (operator().insert(client.getConnection(), record))
                record.setState(DataRowState.None);
        }
    }

    @Override
    public final void updateStorage(DataRow record) throws Exception {
        try (ConnectionClient client = getConnectionClient()) {
            if (operator().update(client.getConnection(), record))
                record.setState(DataRowState.None);
        }
    }

    @Override
    public final void deleteStorage(DataRow record) throws Exception {
        try (ConnectionClient client = getConnectionClient()) {
            if (operator().delete(client.getConnection(), record))
                garbage().remove(record);
        }
    }

    /**
     * 注意：必须使用try finally结构！！！
     * 
     * @return 返回 ConnectionClient 接口对象
     */
    private final ConnectionClient getConnectionClient() {
        return (ConnectionClient) server().getClient();
    }

    public final SqlOperator operator() {
        if (operator == null)
            operator = new SqlOperator(this, sqlServerType);
        if (operator.table() == null) {
            String sqlText = this.sqlText();
            if (sqlText != null)
                operator.setTable(SqlText.findTableName(sqlText));
        }
        return operator;
    }

    @Deprecated
    public final SqlOperator getOperator() {
        return operator();
    }

    public final void setOperator(SqlOperator operator) {
        this.operator = operator;
    }

    // 是否批量保存
    @Override
    public final boolean isBatchSave() {
        return super.isBatchSave();
    }

    @Override
    public final void setBatchSave(boolean batchSave) {
        super.setBatchSave(batchSave);
    }

    /**
     * 增加sql指令内容，调用此函数需要自行解决sql注入攻击！
     *
     * @param sql 要增加的sql指令内容
     * @return 返回对象本身
     */
    public SqlQuery add(String sql) {
        this.sql.add(sql);
        return this;
    }

    public SqlQuery add(String format, Object... args) {
        this.sql.add(format, args);
        return this;
    }

    public SqlWhere addWhere() {
        return new SqlWhere(this.sql());
    }

    public final String sqlText() {
        return this.sql.text();
    }

    @Deprecated
    public final String getCommandText() {
        return sqlText();
    }

    public final SqlText sql() {
        return this.sql;
    }

    @Deprecated
    public final SqlText getSqlText() {
        return sql();
    }

    protected final void setSql(SqlText sqlText) {
        this.sql = sqlText;
    }

    @Deprecated
    protected final void setSqlText(SqlText sqlText) {
        this.setSql(sqlText);
    }

    public final boolean active() {
        return active;
    }

    @Deprecated
    public final boolean isActive() {
        return active();
    }

    private final void setActive(boolean value) {
        this.active = value;
    }

    public final int maximum() {
        return sql().maximum();
    }

    @Deprecated
    public final int getMaximum() {
        return maximum();
    }

    public final SqlQuery setMaximum(int maximum) {
        sql().setMaximum(maximum);
        return this;
    }

    public final boolean isFetchFinish() {
        return fetchFinish;
    }

    protected final void setFetchFinish(boolean fetchFinish) {
        this.fetchFinish = fetchFinish;
    }

    public final ISqlServer server() {
        switch (sqlServerType) {
        case Mysql: {
            if (server != null)
                return server;

            if (master == null)
                master = (MysqlServer) getSession().getProperty(MysqlServerMaster.SessionId);
            if (this.storage()) {
                return master;
            } else {
                if (salve == null) {
                    salve = (MysqlServer) getSession().getProperty(MysqlServerSlave.SessionId);
                    if (salve == null)
                        salve = master;
                    if (salve.getHost().equals(master.getHost()))
                        salve = master;
                }
                return salve;
            }
        }
        case Mssql: {
            if (server == null)
                server = (MssqlServer) getSession().getProperty(MssqlServer.SessionId);
            return server;
        }
        case Sqlite: {
            if (server == null)
                server = new SqliteServer();
            return server;
        }
        default:
            throw new SqlServerTypeException();
        }
    }

    @Deprecated
    protected final ISqlServer getServer() {
        return server();
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public SqlQuery setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

    public SqlServerType getSqlServerType() {
        return sqlServerType;
    }
}
