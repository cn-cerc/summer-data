package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

@SuppressWarnings("deprecation")
public class TDateTest {

    @Test
    public void test_Today() {
        TDate obj = TDate.today();
        assertEquals(obj.getDate(), TDateTime.now().getDate());
    }
}
