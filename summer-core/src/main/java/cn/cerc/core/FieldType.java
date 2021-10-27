package cn.cerc.core;

import java.util.HashMap;
import java.util.Map;

public final class FieldType {
    private String dataType = null;
    private int length = 0; // 包含decimal的长度
    private int decimal = 0; // 小数点位数

    private static Map<String, String> names = new HashMap<>();

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
    public FieldType clone() {
        FieldType result = new FieldType();
        result.dataType = this.dataType;
        result.length = this.length;
        result.decimal = this.decimal;
        return result;
    }

    public static String getName(String key) {
        return names.get(key);
    }

    @Override
    public String toString() {
        if (decimal <= 0) {
            return length > 0 ? dataType + length : dataType;
        } else {
            return dataType + length + "," + decimal;
        }
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
                dataType.substring(1, dataType.length());
            return Integer.parseInt(size) >= 0;
        }
        case "n": {
            if (dataType.length() < 2)
                return false;
            String size = dataType.substring(1, dataType.length());
            if ((!"1".equals(size)) && (!"2".equals(size)))
                return false;
            return true;
        }
        case "f": {
            String[] params = dataType.split(",");
            if (params.length > 2)
                return false;
            String size = params[0].substring(1, params[0].length());
            if ((!"1".equals(size)) && (!"2".equals(size)))
                return false;
            return true;
        }
        default:
            return false;
        }
    }

    public FieldType setType(String dataType) {
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

    public FieldType setType(Class<?> clazz) {
        if (Boolean.class == clazz)
            this.updateType("b", 0);
        else if (FastDate.class.isAssignableFrom(clazz))
            this.updateType("d", 0);
        else if (FastTime.class.isAssignableFrom(clazz))
            this.updateType("t", 0);
        else if ((Datetime.class.isAssignableFrom(clazz)) || java.util.Date.class == clazz)
            this.updateType("dt", 0);
        else if (String.class == clazz) {
            this.updateType("s", 0);
        } else if (Integer.class == clazz) {
            this.updateType("n", 1);
        } else if (Long.class == clazz) {
            this.updateType("n", 2);
        } else if (Float.class == clazz) {
            this.updateType("f", 1);
        } else if (Double.class == clazz) {
            this.updateType("f", 2);
        } else {
            this.updateType("o", 0);
        }
        return this;
    }

    public FieldType put(Object data) {
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
            this.setType(data.getClass());

        return this;
    }

    private void updateType(String dataType, int length) {
        if (this.dataType == null) {
            this.dataType = dataType;
            this.length = length;
            return;
        }

        if ("o".equals(this.dataType))
            return;

        if ("o".equals(dataType)) {
            this.dataType = dataType;
            this.length = 0;
            return;
        }

        if ("f".equals(dataType) && "n".equals(this.dataType)) {
            this.dataType = dataType;
            this.length = length;
            return;
        }
        if ("n".equals(dataType) && "f".equals(this.dataType)) {
            return;
        }

        if (!dataType.equals(this.dataType))
            throw new RuntimeException(String.format("dataType not update from %s to: %s", this.dataType, dataType));

        if (length > this.length)
            this.length = length;
    }

    public final int getLength() {
        return length;
    }

    public final int getDecimal() {
        return decimal;
    }

    public final FieldType setDecimal(int decimal) {
        this.decimal = decimal;
        return this;
    }

    public final FieldType setLength(int length) {
        this.length = length;
        return this;
    }

}
