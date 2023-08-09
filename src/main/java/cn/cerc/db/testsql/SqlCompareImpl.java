package cn.cerc.db.testsql;

import cn.cerc.db.core.Variant;

public interface SqlCompareImpl {

    boolean pass(Variant targe);

    String field();

}
