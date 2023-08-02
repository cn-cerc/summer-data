package cn.cerc.db.testsql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)

@SuiteClasses({ TestsqlServerTest.class, SqlWhereFilterTest.class, TestsqlQueryTest.class })

public class TestsqlTestAll {

}
