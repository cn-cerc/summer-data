package cn.cerc.db.core;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class Variant {
    private String key;
    private Object value;
    private transient boolean modified;

    public Variant() {
        super();
    }

    public Variant(Object data) {
        super();
        this.setValue(data);
    }

    public final String key() {
        return this.key;
    }

    public Object value() {
        return value;
    }

    public Variant setValue(Object value) {
        if (this.value == value)
            return this;
        if (this.value == null && value != null)
            modified = true;
        else if (this.value != null && value == null)
            modified = true;
        else if (!this.value.equals(value))
            modified = true;
        this.value = value;
        return this;
    }

    public Variant setKey(String key) {
        this.key = key;
        return this;
    }

    public final String getString() {
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

    public final boolean getBoolean() {
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

    public final int getInt() {
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

    public final long getLong() {
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

    public final float getFloat() {
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

    public final double getDouble() {
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

    public final BigInteger getBigInteger() {
        Object value = this.value();
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else
            return BigInteger.valueOf(getLong());
    }

    public final BigDecimal getBigDecimal() {
        Object value = this.value();
        if (value instanceof BigDecimal)
            return (BigDecimal) value;
        else
            return BigDecimal.valueOf(getDouble());
    }

    public final Datetime getDatetime() {
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

    public final FastDate getFastDate() {
        return this.getDatetime().toFastDate();
    }

    public final FastTime getFastTime() {
        return this.getDatetime().toFastTime();
    }

    public final <T extends Enum<T>> T getEnum(Class<T> clazz) {
        int tmp = getInt();
        T[] list = clazz.getEnumConstants();
        if (tmp >= 0 && tmp < list.length)
            return list[tmp];
        else
            throw new RuntimeException(String.format("error enum %d of %s", tmp, clazz.getName()));
    }

    @Override
    public final String toString() {
        return String.format("{\"key\":\"%s\",\"value\":\"%s\"}", this.key, this.value());
    }

    public final boolean isModified() {
        return modified;
    }

    protected final void setModified(boolean modified) {
        this.modified = modified;
    }

    public <E extends Enum<E>> void writeToEntity(Object entity, Field field) throws IllegalAccessException {
        if (this.value == null)
            return;
        if ("boolean".equals(field.getType().getName()))
            field.setBoolean(entity, this.getBoolean());
        else if ("int".equals(field.getType().getName()))
            field.setInt(entity, this.getInt());
        else if ("long".equals(field.getType().getName()))
            field.setLong(entity, this.getLong());
        else if ("float".equals(field.getType().getName()))
            field.setDouble(entity, this.getFloat());
        else if ("double".equals(field.getType().getName()))
            field.setDouble(entity, this.getDouble());
        else if (field.getType() == Boolean.class)
            field.set(entity, Boolean.valueOf(this.getBoolean()));
        else if (field.getType() == Integer.class)
            field.set(entity, Integer.valueOf(this.getInt()));
        else if (field.getType() == Long.class)
            field.set(entity, Long.valueOf(this.getLong()));
        else if (field.getType() == Float.class)
            field.set(entity, Float.valueOf(this.getFloat()));
        else if (field.getType() == Double.class)
            field.set(entity, Double.valueOf(this.getDouble()));
        else if (field.getType() == Datetime.class)
            field.set(entity, this.getDatetime());
        else if (field.getType() == FastDate.class)
            field.set(entity, this.getFastDate());
        else if (field.getType() == FastTime.class)
            field.set(entity, this.getFastTime());
        else if (field.getType() == String.class)
            field.set(entity, this.getString());
        else if (field.getType().isEnum()) {
            @SuppressWarnings("unchecked")
            Class<E> enumType = (Class<E>) field.getType();
            field.set(entity, this.getEnum(enumType));
        } else {
            if (this.value() != null)
                throw new RuntimeException(String.format("field %s error: %s as %s", field.getName(),
                        this.value().getClass().getName(), field.getType().getName()));
            else
                throw new RuntimeException(
                        String.format("field %s error: %s to null", field.getName(), field.getType().getName()));
        }
    }

    public boolean hasValue() {
        return !"".equals(getString());
    }

    public static void main(String[] args) {
        DataSet ds = new DataSet();
        ds.append().setValue("code", 1);
        ds.append().setValue("code", 2);
        var code1 = ds.bindColumn("code");
        var code2 = ds.current().bind("code");
        System.out.println(code1);
        System.out.println(code2);
        ds.first();
        System.out.println(code1);
        System.out.println(code2);
    }

}
