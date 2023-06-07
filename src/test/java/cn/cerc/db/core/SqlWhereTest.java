package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.SqlWhere.LinkOptionEnum;

public class SqlWhereTest {
    private SqlWhere where;

    @Before
    public void setUp() throws Exception {
        where = new SqlWhere();
    }

    @Test
    public void test_sqlText() {
        SqlText sql = new SqlText(SqlServerType.Mysql);
        sql.add("select * from xxx");
        sql.addWhere().eq("code_", "abc").eq("name_", "0001").like("remark_", "a").build();
        assertEquals("select * from xxx where code_='abc' and name_='0001' and remark_ like 'a%'", sql.text());
    }

    @Test
    public void void_test_eq() {
        assertEquals("code=''", where.eq("code", "").toString());
        assertEquals("code is null", where.clear().eq("code", null).toString());
    }

    @Test
    public void void_test_neq() {
        assertEquals("code<>''", where.neq("code", "").toString());
        assertEquals("code is not null", where.clear().neq("code", null).toString());
    }

    @Test
    public void test_and() {
        assertEquals("code='' and code is null", where.eq("code", "").eq("code", null).toString());
        assertEquals("code=1", where.clear().eq("code", true).toString());
        assertEquals("code='a'", where.clear().eq("code", "a").toString());
        assertEquals("code='a' and name='b'", where.eq("name", "b").toString());
    }

    @Test
    public void test_or() {
        assertEquals("code='' and code is null", where.eq("code", "").eq("code", null).toString());
        assertEquals("code='' and code is null and code='a'", where.eq("code", "a").toString());
        assertEquals("code='' and code is null and code='a' or name='b'", where.or().eq("name", "b").toString());
    }

    @Test
    public void test_like() {
        assertEquals("", where.like("name", "").toString());
        assertEquals("name like 'a%'", where.like("name", "a").toString());
        assertEquals("name like 'a%'", where.clear().like("name", "a*").toString());
        assertEquals("name like '%a'", where.clear().like("name", "*a").toString());
        assertEquals("name like '%a%'", where.clear().like("name", "*a*").toString());
        assertEquals("name like '%a%'", where.clear().like("name", "a", LinkOptionEnum.All).toString());
    }

    @Test
    public void test_in() {
        assertEquals("value in (0,1,2)", where.in("value", List.of(0, 1, 2)).toString());
        where.clear();
        assertEquals("value in ('a','b','c')", where.in("value", List.of("a", "b", "c")).toString());
    }

    @Test
    public void test_inGroup() {
        List<Object[]> list = new ArrayList<>();
        assertEquals("", where.clear().inGroup(List.of("v1", "v2"), list).toString());
        list.add(new Object[] { 1, 2 });
        assertEquals("((v1=1 and v2=2))", where.clear().inGroup(List.of("v1", "v2"), list).toString());
        list.add(new Object[] { 1, 3 });
        assertEquals("((v1=1 and v2=2) or (v1=1 and v2=3))",
                where.clear().inGroup(List.of("v1", "v2"), list).toString());
    }

    @Test
    public void test_between() {
        assertEquals("value between 1 and 10", where.between("value", 1, 10).toString());
        assertEquals("value between '2020-01-01' and '2020-12-30'",
                where.clear().between("value", new FastDate("2020-01-01"), new FastDate("2020-12-30")).toString());
        assertEquals("value between 'a' and 'b'", where.clear().between("value", "a", "b").toString());
    }

    @Test
    public void test_is() {
        assertEquals("value is null", where.clear().isNull("value", true).toString());
        assertEquals("value is not null", where.clear().isNull("value", false).toString());
    }

    @Test
    public void test_headIn() {
        DataRow row = new DataRow();
        row.setValue("value", true);
        row.setValue("code", "a");
        where.setDataRow(row);
        assertEquals("value is null", where.clear().isNull("value").toString());
        assertEquals("code='a'", where.clear().eq("code").toString());
    }

    @Test
    public void test_OR() {
        where.eq("code", 1).or().eq("code", 2).AND().eq("name", "A").or().eq("name", "B");
        assertEquals("(code=1 or code=2) AND (name='A' or name='B')", where.toString());
    }

    @Test
    public void test_fn() {
        long s1 = System.nanoTime();
        where.eq(Car::getName, "宝马");
        long s2 = System.nanoTime();
        where.clear();
        long s3 = System.nanoTime();
        where.eq("Name_", "宝马");
        long s4 = System.nanoTime();
        System.out.println(s2 - s1);
        System.out.println(s4 - s3);
    }

    public static class Car {
        private String name_;

        public String getName() {
            return name_;
        }

        public void setName_(String name_) {
            this.name_ = name_;
        }
    }
}
