package cn.cerc.mis.log;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

public class JayunLogParserTest {

    public final static String WantedName = "WantedName";

    private final static String line = "at cn.cerc.mis.log.WantedTest.test(WantedTest.java:9)";

    @Test
    public void trigger_test() {
        assertEquals("cn.cerc.mis.log.WantedTest", JayunLogParser.trigger(line));
    }

    @Test
    public void lineNumber_test() {
        assertEquals("9", JayunLogParser.lineNumber(line));
    }

    @Test
    public void getJayunLogData_test() {
        try {
            new WantedTest().test();
        } catch (Exception e) {
            Optional<JayunLogData> jayunLogData = JayunLogParser.getJayunLogData(getClass(), e, JayunLogData.error);
            jayunLogData.ifPresent(data -> {
                assertEquals(data.getName(), JayunLogParserTest.WantedName);
                assertEquals(data.getId(), "cn.cerc.mis.log.WantedTest");
                assertEquals(data.getLine(), "9");
                assertEquals(data.getMessage(), "/ by zero");
            });
        }
    }

}
