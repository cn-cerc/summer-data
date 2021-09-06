package cn.cerc.core;

import java.math.BigDecimal;
import java.math.BigInteger;

@Deprecated
public interface IRecord {

    boolean exists(String field);

    boolean getBoolean(String field);

    int getInt(String field);

    BigInteger getBigInteger(String field);

    BigDecimal getBigDecimal(String field);

    double getDouble(String field);

    String getString(String field);

    TDateTime getDateTime(String field);

    Object setField(String field, Object value);

    Object getField(String field);

}
