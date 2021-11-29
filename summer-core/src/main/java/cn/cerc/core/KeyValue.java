package cn.cerc.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.google.gson.Gson;

public class KeyValue {
    private String _key;
    private Object _value;

    public KeyValue() {
        super();
    }

    public KeyValue(Object value) {
        super();
        this.setValue(value);
    }

    public final String key() {
        return this._key;
    }

    @Deprecated
    public String getKey() {
        return key();
    }

    public final Object value() {
        return this._value;
    }

    public KeyValue setValue(Object data) {
        this._value = data;
        return this;
    }

    public final KeyValue setKey(String value) {
        this._key = value;
        return this;
    }

    public final String asString() {
        Object value = this.value();
        if (value == null) {
            return "";
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Date) {
            Datetime tmp = new Datetime((Date) value);
            return tmp.toString();
        } else if (value instanceof Float || value instanceof Double) {
            String str = value.toString();
            if (str.endsWith(".0"))
                return str.substring(0, str.length() - 2);
            else
                return str;
        } else {
            return value.toString();
        }
    }

    public final boolean asBoolean() {
        Object value = this.value();
        if (value == null) {
            return false;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if ((value instanceof Short)) {
            return ((Short) value).intValue() > 0;
        } else if (value instanceof Integer) {
            return (Integer) value > 0;
        } else if (value instanceof String) {
            String str = (String) value;
            return !"".equals(str) && !"0".equals(str) && !"false".equals(str);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    public final int asInt() {
        Object value = this.value();
        if (value == null) {
            return 0;
        } else if ((value instanceof Boolean)) {
            return (Boolean) value ? 1 : 0;
        } else if ((value instanceof Short)) {
            return ((Short) value).intValue();
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            Long tmp = (Long) value;
            if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                throw new ClassCastException("Long to Integer is out of range");
            return tmp.intValue();
        } else if (value instanceof Float) {
            Float tmp = (Float) value;
            if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                throw new ClassCastException("Float to Integer is out of range");
            if (tmp != tmp.intValue())
                throw new ClassCastException("Float to Integer fail, value: " + tmp);
            return tmp.intValue();
        } else if (value instanceof Double) {
            Double tmp = (Double) value;
            if (tmp < Integer.MIN_VALUE || tmp > Integer.MAX_VALUE)
                throw new ClassCastException("Double to Integer is out of range");
            if (tmp != tmp.intValue())
                throw new ClassCastException("Double to Integer fail, value: " + tmp);
            return tmp.intValue();
        } else if (value instanceof String) {
            String str = (String) value;
            if ("".equals(str))
                return 0;
            return Integer.parseInt(str);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    public final long asLong() {
        Object value = this.value();
        if (value == null) {
            return 0;
        } else if ((value instanceof Boolean)) {
            return (Boolean) value ? 1 : 0;
        } else if ((value instanceof Short)) {
            return ((Short) value);
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            return ((Long) value);
        } else if (value instanceof Float) {
            Float tmp = (Float) value;
            if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                throw new ClassCastException("Float to Long is out of range");
            if (tmp != tmp.longValue())
                throw new ClassCastException("Float to Long fail, value: " + tmp);
            return tmp.longValue();
        } else if (value instanceof Double) {
            Double tmp = (Double) value;
            if (tmp < Long.MIN_VALUE || tmp > Long.MAX_VALUE)
                throw new ClassCastException("Double to Long is out of range");
            if (tmp != tmp.longValue())
                throw new ClassCastException("Double to Long fail, value: " + tmp);
            return tmp.longValue();
        } else if (value instanceof String) {
            String str = (String) value;
            if ("".equals(str))
                return 0;
            return Long.parseLong(str);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    public final float asFloat() {
        Object value = this.value();
        if (value == null) {
            return 0;
        } else if ((value instanceof Boolean)) {
            return (Boolean) value ? 1 : 0;
        } else if ((value instanceof Short)) {
            return ((Short) value);
        } else if (value instanceof Integer) {
            return ((Integer) value);
        } else if (value instanceof Long) {
            Long tmp = (Long) value;
            if (tmp < Float.MIN_VALUE || tmp > Float.MAX_VALUE)
                throw new ClassCastException("Long to Float is out of range");
            return tmp.floatValue();
        } else if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            Double tmp = (Double) value;
            if (tmp < Float.MIN_VALUE || tmp > Float.MAX_VALUE)
                throw new ClassCastException("Double to Float is out of range");
            return tmp.floatValue();
        } else if (value instanceof String) {
            String str = (String) value;
            if ("".equals(str))
                return 0;
            return Float.parseFloat(str);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    public final double asDouble() {
        Object value = this.value();
        if (value == null) {
            return 0;
        } else if ((value instanceof Boolean)) {
            return (Boolean) value ? 1 : 0;
        } else if ((value instanceof Short)) {
            return ((Short) value);
        } else if (value instanceof Integer) {
            return ((Integer) value);
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof String) {
            String str = (String) value;
            if ("".equals(str))
                return 0;
            return Double.parseDouble(str);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    public final BigInteger asBigInteger() {
        Object value = this.value();
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else
            return BigInteger.valueOf(asLong());
    }

    public final BigDecimal asBigDecimal() {
        Object value = this.value();
        if (value instanceof BigDecimal)
            return (BigDecimal) value;
        else
            return BigDecimal.valueOf(asDouble());
    }

    public final Datetime asDatetime() {
        Object value = this.value();
        if (value == null) {
            return Datetime.zero();
        } else if (value instanceof Datetime) {
            return (Datetime) value;
        } else if (value instanceof Date) {
            return new Datetime((Date) value);
        } else if (value instanceof String) {
            return new Datetime((String) value);
        } else {
            throw new ClassCastException(String.format("%s not support %s.", value.getClass().getName()));
        }
    }

    public final FastDate asFastDate() {
        return this.asDatetime().toFastDate();
    }

    public final FastTime asFastTime() {
        return this.asDatetime().toFastTime();
    }

    @Override
    public final String toString() {
        return new Gson().toJson(this);
    }

    public static void main(String[] args) {
        System.out.println(new KeyValue());
        System.out.println(new KeyValue("202109"));
        System.out.println(new KeyValue("202109").setKey("date"));

        KeyValue kv = new KeyValue("3").setKey("id");
        System.out.println(kv.key());
    }

}
