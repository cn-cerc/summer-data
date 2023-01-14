package cn.cerc.db.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.BigdataException;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;

public class SqlQueryTest implements IHandle {
    private static final Logger log = LoggerFactory.getLogger(SqlQueryTest.class);

    private String userInfo;
    private MysqlQuery ds;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
        ds = new MysqlQuery(this);
        userInfo = "Account";
    }

    @Test
    @Ignore
    public void test_open() {
        ds.sql().setMaximum(1);
        ds.add("select Code_,Name_ from %s", userInfo);
        ds.open();
        assertEquals(ds.size(), 1);
    }

    @Test
    public void test_open_getSelectCommand() {
        String sql;
        sql = String.format("select Code_,Name_ from %s", userInfo);
        ds.add(sql);
        assertEquals(String.format("%s limit %s", sql, (BigdataException.MAX_RECORDS + 2)), ds.sql().getCommand());

        ds = new MysqlQuery(this);
        sql = String.format("select Code_,Name_ from %s limit 1", userInfo);
        ds.sql().setMaximum(-1);
        ds.add(sql);
        assertEquals(sql, ds.sql().getCommand());

        ds = new MysqlQuery(this);
        ds.sql().setMaximum(BigdataException.MAX_RECORDS);
        sql = String.format("select Code_,Name_ from %s", userInfo);
        ds.add(sql);

        assertEquals(String.format("%s limit %s", sql, (BigdataException.MAX_RECORDS + 2)), ds.sql().getCommand());
    }

    @Test
    @Ignore
    public void test_open_2() {
        ds.sql().setMaximum(1);
        String str = "\\小王233\\";
        ds.add("select * from %s where Name_='%s'", userInfo, str);
        log.info(ds.sql().text());
        ds.open();

        ds.clear();
        str = "\\\\小王233\\\\";
        ds.add("select * from %s where Name_='%s'", userInfo, str);
        log.info(ds.sql().text());
        ds.open();

    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

}
