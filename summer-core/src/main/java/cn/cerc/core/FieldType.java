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
        names.put("d", "TDate"); // TDate
        names.put("t", "TDateTime"); // TDateTime or Date
        names.put("s", "string"); // nvarchar
        names.put("n", "numeric"); // 整数 integer or long
        names.put("f", "float"); // 浮点 float or double
        names.put("o", "other"); // other object
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
        case "d":
        case "t": {
            return dataType.length() == 1;
        }
        case "s": {
            if (dataType.length() < 2)
                return false;
            String size = dataType.substring(1, dataType.length());
            return Integer.parseInt(size) > 0;
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
        case "d":
        case "t": {
            updateType(t, 0);
            break;
        }
        case "s": {
            String size = dataType.substring(1, dataType.length());
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
        else if (TDate.class == clazz)
            this.updateType("d", 0);
        else if ((TDateTime.class == clazz) || (java.util.Date.class == clazz))
            this.updateType("t", 0);
        else if (String.class == clazz) {
            this.updateType("s", 1);
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

        if (!dataType.equals(this.dataType))
            throw new RuntimeException("dataType not update");

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

    public static void main(String[] args) {
        System.out.println(new FieldType().setType("b"));
        System.out.println(new FieldType().setType("d"));
        System.out.println(new FieldType().setType("t"));
        System.out.println(new FieldType().setType("s10"));
        System.out.println(new FieldType().setType("n1"));
        System.out.println(new FieldType().setType("n2"));
        System.out.println(new FieldType().setType("f2"));
        System.out.println(new FieldType().setType("f1,5"));

        System.out.println("*****");
        System.out.println(new FieldType().setType(Boolean.class));
        System.out.println(new FieldType().setType(TDate.class));
        System.out.println(new FieldType().setType(TDateTime.class));
        System.out.println(new FieldType().setType(String.class).setLength(50));
        System.out.println(new FieldType().setType(Integer.class));
        System.out.println(new FieldType().setType(Long.class));
        System.out.println(new FieldType().setType(Float.class).setDecimal(4));
        System.out.println(new FieldType().setType(Double.class).setDecimal(4));

        System.out.println("*****");
        System.out.println(new FieldType().put(true));
        System.out.println(new FieldType().put(TDateTime.now().asDate()));
        System.out.println(new FieldType().put(TDateTime.now()));
        System.out.println(new FieldType().put("a").put("abc"));
        System.out.println(new FieldType().put(1));
        System.out.println(new FieldType().put(12121l));
        System.out.println(new FieldType().put(12121f));
        System.out.println(new FieldType().put(12121.32f));
        System.out.println(new FieldType().put(12.323).put(12.3233));
//        System.out.println(new FieldType().put(new Date()));
    }
}
