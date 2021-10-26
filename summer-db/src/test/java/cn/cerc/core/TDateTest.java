package cn.cerc.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.core.TDate;
import cn.cerc.db.core.TDateTime;

@SuppressWarnings("deprecation")
public class TDateTest {

    @Test
    public void test_Today() {
        TDate obj = TDate.today();
        assertEquals(obj.getDate(), TDateTime.now().getDate());
    }
}
