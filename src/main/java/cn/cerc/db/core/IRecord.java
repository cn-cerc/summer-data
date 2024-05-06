package cn.cerc.db.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public interface IRecord {

    boolean exists(String field);

    Object setValue(String field, Object value);

    Object getValue(String field);

    default String getString(String field) {
        Object value = this.getValue(field);
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

    default boolean getBoolean(String field) {
        Object value = this.getValue(field);
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

    default int getInt(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            return 0;
        } else if (value instanceof Boolean item) {
            return item ? 1 : 0;
        } else if (value instanceof Short item) {
            return item.intValue();
        } else if (value instanceof Integer item) {
            return item;
        } else if (value instanceof Long item) {
            if (item < Integer.MIN_VALUE || item > Integer.MAX_VALUE)
                throw new ClassCastException("Long to Integer is out of range");
            return item.intValue();
        } else if (value instanceof BigInteger item) {
            return item.intValue();
        } else if (value instanceof Float item) {
            if (item < Integer.MIN_VALUE || item > Integer.MAX_VALUE)
                throw new ClassCastException("Float to Integer is out of range");
            if (item != item.intValue())
                throw new ClassCastException("Float to Integer fail, value: " + item);
            return item.intValue();
        } else if (value instanceof Double item) {
            if (item < Integer.MIN_VALUE || item > Integer.MAX_VALUE)
                throw new ClassCastException("Double to Integer is out of range");
            if (item != item.intValue())
                throw new ClassCastException(
                        String.format("Double to Integer fail, original %s -> %s ", item, item.intValue()));
            return item.intValue();
        } else if (value instanceof String str) {
            if (str.isEmpty())
                return 0;
            return Double.valueOf(str).intValue();
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    default long getLong(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            return 0;
        } else if (value instanceof Boolean item) {
            return item ? 1 : 0;
        } else if (value instanceof Short item) {
            return item.longValue();
        } else if (value instanceof Integer item) {
            return item.longValue();
        } else if (value instanceof Long item) {
            return item;
        } else if (value instanceof BigInteger item) {
            return item.longValue();
        } else if (value instanceof Float item) {
            if (item < Long.MIN_VALUE || item > Long.MAX_VALUE)
                throw new ClassCastException("Float to Long is out of range");
            if (item != item.longValue())
                throw new ClassCastException(
                        String.format("Float to Long fail, original %s -> %s ", item, item.longValue()));
            return item.longValue();
        } else if (value instanceof Double item) {
            if (item < Long.MIN_VALUE || item > Long.MAX_VALUE)
                throw new ClassCastException("Double to Long is out of range");
            if (item != item.longValue())
                throw new ClassCastException(
                        String.format("Double to Long fail, original %s -> %s ", item, item.longValue()));
            return item.longValue();
        } else if (value instanceof String item) {
            if (item.isEmpty())
                return 0;
            return Long.parseLong(item);
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    default float getFloat(String field) {
        Object value = this.getValue(field);
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

    default double getDouble(String field, int scale) {
        return Utils.roundTo(getDouble(field), scale);
    }

    default double getDouble(String field) {
        Object value = this.getValue(field);
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
            try {
                return new BigDecimal(str).doubleValue();
            } catch (Exception e) {
                throw new NumberFormatException(String.format("Error converting value %s to double", str));
            }
        } else {
            throw new ClassCastException(String.format("not support class: %s", value.getClass().getName()));
        }
    }

    default BigInteger getBigInteger(String field) {
        Object value = this.getValue(field);
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else
            return BigInteger.valueOf(getLong(field));
    }

    default BigDecimal getBigDecimal(String field) {
        Object value = this.getValue(field);
        if (value instanceof BigDecimal)
            return (BigDecimal) value;
        else
            return BigDecimal.valueOf(getDouble(field));
    }

    default Datetime getDatetime(String field) {
        Object value = this.getValue(field);
        if (value == null) {
            return Datetime.zero();
        } else if (value instanceof Datetime) {
            return (Datetime) value;
        } else if (value instanceof Date) {
            return new Datetime((Date) value);
        } else if (value instanceof String) {
            return new Datetime((String) value);
        } else {
            throw new ClassCastException(String.format("%s field not support %s.", field, value.getClass().getName()));
        }
    }

    default FastDate getFastDate(String field) {
        return this.getDatetime(field).toFastDate();
    }

    default FastTime getFastTime(String field) {
        return this.getDatetime(field).toFastTime();
    }

    default <T extends Enum<?>> T getEnum(String field, Class<T> clazz) {
        final int index = getInt(field);
        T[] list = clazz.getEnumConstants();
        if (index >= 0 && index < list.length)
            return list[index];
        else
            throw new RuntimeException(String.format("error enum %d of %s", index, clazz.getName()));
    }
}
