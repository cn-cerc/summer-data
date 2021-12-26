package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.core.DataRow;
import cn.cerc.core.FastDate;
import cn.cerc.db.core.SqlWhere.LinkOptionEnum;
import cn.cerc.db.mysql.MysqlQuery;

public class SqlWhereTest {
    private SqlWhere where;

    @Before
    public void setUp() throws Exception {
        where = new SqlWhere();
    }

    @Test
    public void test_sqlText() {
        MysqlQuery query = new MysqlQuery(null);
        query.add("select * from xxx");
        query.addWhere().eq("code_", "abc").eq("name_", "0001").like("remark_", "a").build();
        assertEquals("select * from xxx where code_='abc' and name_='0001' and remark_ like 'a%%'", query.sqlText());
    }

    @Test
    public void test_and() {
        assertEquals("", where.eq("code", "").eq("code", null).text());
        assertEquals("code=1", where.clear().eq("code", true).text());
        assertEquals("code='a'", where.clear().eq("code", "a").text());
        assertEquals("code='a' and name='b'", where.eq("name", "b").text());
    }

    @Test
    public void test_or() {
        assertEquals("", where.eq("code", "").eq("code", null).text());
        assertEquals("code='a'", where.eq("code", "a").text());
        assertEquals("code='a' or name='b'", where.or().eq("name", "b").text());
    }

    @Test
    public void test_like() {
        assertEquals("", where.like("name", "").text());
        assertEquals("name like 'a%%'", where.like("name", "a").text());
        assertEquals("name like 'a%%'", where.clear().like("name", "a*").text());
        assertEquals("name like '%%a'", where.clear().like("name", "*a").text());
        assertEquals("name like '%%a%%'", where.clear().like("name", "*a*").text());
        assertEquals("name like '%%a%%'", where.clear().like("name", "a", LinkOptionEnum.All).text());
    }

    @Test
    public void test_in() {
        assertEquals("value in (0,1)", where.in("value", 0, 1).text());
    }

    @Test
    public void test_between() {
        assertEquals("value between 1 and 10", where.between("value", 1, 10).text());
        assertEquals("value between '2020-01-01' and '2020-12-30'",
                where.clear().between("value", new FastDate("2020-01-01"), new FastDate("2020-12-30")).text());
        assertEquals("value between 'a' and 'b'", where.clear().between("value", "a", "b").text());
    }

    @Test
    public void test_is() {
        assertEquals("value is null", where.clear().is("value", true).text());
        assertEquals("value is not null", where.clear().is("value", false).text());
    }

    @Test
    public void test_headIn() {
        DataRow row = new DataRow();
        row.setValue("value", true);
        row.setValue("code", "a");
        assertEquals("value is null", where.clear().isNull("value", row).text());
        assertEquals("code='a'", where.clear().eq("code", row).text());
    }
}
