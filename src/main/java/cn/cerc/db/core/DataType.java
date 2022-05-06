package cn.cerc.db.core;

import java.util.HashMap;
import java.util.Map;

public final class DataType {
    private String content = null;
    private int length = 0; // 包含decimal的长度
    private int decimal = 0; // 小数点位数

    private static final Map<String, String> names = new HashMap<>();

    static {
        names.put("b", "boolean");
        names.put("n", "numeric"); // 整数 integer or long
        names.put("f", "float"); // 浮点 float or double
        names.put("s", "string"); // nvarchar
        //
        names.put("d", "FastDate"); // FastDate
        names.put("t", "FastTime"); // FastTime
        names.put("dt", "Dateime"); // Datetime
        //
        names.put("o", "other"); // other object
    }

    @Override
    public DataType clone() {
        DataType result = new DataType();
        result.content = this.content;
        result.length = this.length;
        result.decimal = this.decimal;
        return result;
    }

    public static String getName(String key) {
        return names.get(key);
    }

    @Override
    public String toString() {
        return value();
    }

    public String value() {
        if (content == null)
            return null;
        if (decimal <= 0)
            return length > 0 ? content + length : content;
        else
            return content + length + "," + decimal;
    }

    public boolean validate(String dataType) {
        if (Utils.isEmpty(dataType))
            return false;

        String t = dataType.substring(0, 1);
        switch (t) {
        case "o":
        case "b":
        case "t": {
            return dataType.length() == 1;
        }
        case "d": {
            return dataType.length() == 1 || "t".equals(dataType.substring(1, dataType.length()));
        }
        case "s": {
            String size = "0";
            if (dataType.length() > 1)
                dataType.substring(1);
            return Integer.parseInt(size) >= 0;
        }
        case "n": {
            if (dataType.length() < 2)
                return false;
            String size = dataType.substring(1);
            if ((!"1".equals(size)) && (!"2".equals(size)))
                return false;
            return true;
        }
        case "f": {
            String[] params = dataType.split(",");
            if (params.length > 2)
                return false;
            String size = params[0].substring(1);
            if ((!"1".equals(size)) && (!"2".equals(size)))
                return false;
            return true;
        }
        default:
            return false;
        }
    }

    public DataType setValue(String dataType) {
        if (Utils.isEmpty(dataType))
            return this;

        if (!validate(dataType))
            throw new RuntimeException("dataType is error: " + dataType);

        String t = dataType.substring(0, 1);
        switch (t) {
        case "o":
        case "b":
        case "t":
        case "d": {
            updateType(dataType, 0);
            break;
        }
        case "s": {
            String size = "0";
            if (dataType.length() > 1)
                size = dataType.substring(1, dataType.length());
            updateType(t, Integer.parseInt(size));
            break;
        }
        case "n": {
            String size = dataType.substring(1, dataType.length());
            updateType(t, Integer.parseInt(size));
            break;
        }
        case "f": {
            String[] params = dataType.split(",");
            String size = params[0].substring(1, params[0].length());
            updateType(t, Integer.parseInt(size));
            if (params.length > 1)
                this.decimal = Integer.parseInt(params[1]);
            break;
        }
        default:
            throw new RuntimeException("dataType error: " + dataType);
        }

        return this;
    }

    public DataType readClass(Class<?> clazz) {
        if (boolean.class == clazz || Boolean.class == clazz)
            this.updateType("b", 0);
        else if (int.class == clazz || Integer.class == clazz)
            this.updateType("n", 1);
        else if (long.class == clazz || Long.class == clazz)
            this.updateType("n", 2);
        else if (float.class == clazz || Float.class == clazz)
            this.updateType("f", 1);
        else if (double.class == clazz || Double.class == clazz)
            this.updateType("f", 2);
        else if (FastDate.class.isAssignableFrom(clazz))
            this.updateType("d", 0);
        else if (FastTime.class.isAssignableFrom(clazz))
            this.updateType("t", 0);
        else if ((Datetime.class.isAssignableFrom(clazz)) || java.util.Date.class == clazz)
            this.updateType("dt", 0);
        else if (String.class == clazz)
            this.updateType("s", 0);
        else
            this.updateType("o", 0);
        return this;
    }

    public DataType readData(Object data) {
        if (data == null)
            return this;

        if (data instanceof String) {
            updateType("s", ((String) data).length());
        } else if (data instanceof Float) {
            updateType("f", 1);
            Float value = (Float) data;
            String[] args = value.toString().split("\\.");
            if (!"0".equals(args[1])) {
                Integer dec = args[1].length();
                if (this.decimal < dec)
                    this.decimal = dec;
            }
        } else if (data instanceof Double) {
            updateType("f", 2);
            Double value = (Double) data;
            String[] args = value.toString().split("\\.");
            if (!"0".equals(args[1])) {
                Integer dec = args[1].length();
                if (this.decimal < dec)
                    this.decimal = dec;
            }
        } else
            this.readClass(data.getClass());

        return this;
    }

    private void updateType(String dataType, int length) {
        if (this.content == null) {
            this.content = dataType;
            this.length = length;
            return;
        }

        if ("o".equals(this.content))
            return;

        if ("o".equals(dataType)) {
            this.content = dataType;
            this.length = 0;
            return;
        }

        if ("f".equals(dataType) && "n".equals(this.content)) {
            this.content = dataType;
            this.length = length;
            return;
        }
        if ("n".equals(dataType) && "f".equals(this.content)) {
            return;
        }

        if (!dataType.equals(this.content))
            throw new RuntimeException(String.format("dataType not update from %s to: %s", this.content, dataType));

        if (length > this.length)
            this.length = length;
    }

    public final String dataType() {
        return this.content;
    }

    public final int getLength() {
        return length;
    }

    public final int getDecimal() {
        return decimal;
    }

    public final DataType setDecimal(int decimal) {
        this.decimal = decimal;
        return this;
    }

    public final DataType setLength(int length) {
        if ("s".equals(content) || "o".equals(content))
            this.length = length;
        else if (("n".equals(content) || "f".equals(content)) && (length == 1 || length == 2))
            this.length = length;
        else
            throw new RuntimeException(String.format("the dateType is %s, error length: %s", this.content, length));
        return this;
    }

}
