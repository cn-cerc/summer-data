package cn.cerc.db.mysql;

import cn.cerc.core.DataRow;
import cn.cerc.core.ISession;
import cn.cerc.db.core.IHandle;

/**
 * 用于组合生成select指令，便于多条件查询编写；请改使用MysqlHelper
 *
 * @author 张弓
 */
public class BuildQuery extends QueryHelper<MysqlQuery> {

    public BuildQuery(IHandle owner) {
        super(owner.getSession());
    }

    public BuildQuery(ISession session) {
        super(session);
    }

    public BuildQuery(MysqlQuery query) {
        super(query);
    }

    public BuildQuery add(String fmtText, Object... args) {
        super.addSelect(fmtText, args);
        return this;
    }

    public BuildQuery add(String text) {
        super.addSelect(text);
        return this;
    }

    @Override
    public MysqlQuery dataSet() {
        if (this.dataSet == null)
            this.dataSet = new MysqlQuery(this);
        return this.dataSet;
    }

    @Deprecated
    public final MysqlQuery getDataSet() {
        return dataSet();
    }

    public final String getCommandText() {
        return sqlText();
    }

    @Deprecated
    public final MysqlQuery open(DataRow head, DataRow foot) {
        MysqlQuery ds = dataSet();
        if (!head.exists("__offset__")) {
        } else {
            this.setOffset(head.getInt("__offset__"));
        }
        ds.getSqlText().clear();
        ds.add(this.getSelectCommand());
        ds.open();
        if (foot == null) {
            return ds;
        }
        foot.setValue("__finish__", ds.isFetchFinish());
        return ds;
    }

    @Deprecated
    public final MysqlQuery openReadonly(DataRow head, DataRow foot) {
        MysqlQuery ds = dataSet();
        if (head.exists("__offset__")) {
            this.setOffset(head.getInt("__offset__"));
        }
        ds.getSqlText().clear();
        ds.add(this.getSelectCommand());
        ds.openReadonly();
        if (foot != null) {
            foot.setValue("__finish__", ds.isFetchFinish());
        }
        return ds;
    }

    public final void close() {
        super.clear();
    }

    public final String getOrderText() {
        return this.order();
    }

    public final BuildQuery setOrderText(String orderText) {
        this.setOrder(orderText);
        return this;
    }

    @Deprecated
    public final int getMaximum() {
        return maximum();
    }

    @Deprecated
    public final int getOffset() {
        return offset();
    }

    @Deprecated
    protected final String getSelectCommand() {
        return select();
    }
}
