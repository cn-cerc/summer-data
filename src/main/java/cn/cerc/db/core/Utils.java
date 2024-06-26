package cn.cerc.db.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Transient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static final String vbCrLf = "\r\n";
    public static final String separtor = System.lineSeparator();

    /**
     * 空串
     */
    public static final String EMPTY = "";

    /**
     * 保障查询安全，防范注入攻击
     *
     * @param value 用户输入值
     * @return 经过处理后的值
     */
    public static String safeString(String value) {
        return value == null ? "" : value.replaceAll("'", "''");
    }

    public static String serializeToString(Object obj) throws IOException {
        if (obj == null) {
            return null;
        }
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
        objOut.writeObject(obj);
        return byteOut.toString(StandardCharsets.ISO_8859_1);// 此处只能是ISO-8859-1,但是不会影响中文使用;
    }

    public static Object deserializeToObject(String str) throws IOException, ClassNotFoundException {
        if (str == null) {
            return null;
        }
        ByteArrayInputStream byteIn = new ByteArrayInputStream(str.getBytes(StandardCharsets.ISO_8859_1));
        ObjectInputStream objIn = new ObjectInputStream(byteIn);
        return objIn.readObject();
    }

    /**
     * 按照指定的编码格式进行url编码
     *
     * @param value 原始字符串
     * @param enc   编码格式 StandardCharsets.UTF_8.name()
     * @return 编码后的字符串
     */
    public static String encode(String value, String enc) {
        try {
            return URLEncoder.encode(value, enc);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex.getCause());
        }
    }

    /**
     * 按照指定的编码格式进行url解码，建议使用jdk标准的枚举类型
     *
     * @param value 原始字符串
     * @param enc   编码格式 StandardCharsets.UTF_8.name()
     * @return 解码后的字符串
     */
    @Deprecated
    public static String decode(String value, String enc) {
        try {
            return URLDecoder.decode(value, enc);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex.getCause());
        }
    }

    /**
     * 按照指定的编码格式进行url解码
     *
     * @param value   原始字符串
     * @param charset 编码格式 StandardCharsets.UTF_8
     * @return 解码后的字符串
     */
    public static String decode(String value, Charset charset) {
        return URLDecoder.decode(value, charset);
    }

    public static String encode(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
            objOut.writeObject(obj);
            return byteOut.toString(StandardCharsets.ISO_8859_1);// 此处只能是ISO-8859-1,但是不会影响中文使用;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object decode(String str) {
        if (str == null) {
            return null;
        }
        try {
            ByteArrayInputStream byteIn = new ByteArrayInputStream(str.getBytes(StandardCharsets.ISO_8859_1));
            ObjectInputStream objIn = new ObjectInputStream(byteIn);
            return objIn.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 兼容 delphi 代码
    public static int round(double d) {
        if (LanguageResource.isLanguageTW()) {
            return new BigDecimal(d).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        return (int) Math.round(d);
    }

    /**
     * double 类型数字格式化
     *
     * @param val   将要格式化的数字
     * @param scale 精确到的位置 负数表示小数向后的位数，例如：2351.2513 <br>
     *              当scale = -2时，精确后为2351.25 <br>
     *              当scale = -1时，精确后为2351.3 正数表示小数向前的位数，例如：2351.2513 <br>
     *              当scale = 2时，精确后为2400.0 当scale = 3时，精确后为2000.0 <br>
     * @return 指定小数点的四舍五入
     */
    public static double roundTo(double val, int scale) {
        try {
            BigDecimal bigDecimal = new BigDecimal(Double.toString(val));
            return bigDecimal.setScale(-scale, RoundingMode.HALF_UP).doubleValue();
        } catch (NumberFormatException e) {
            log.error("val {}, scale {}, error {}", val, scale, e.getMessage(), e);
            return 0;
        }
    }

    // 兼容 delphi 代码
    public static int pos(String sub, String text) {
        return text.indexOf(sub) + 1;
    }

    // 兼容 delphi 代码
    public static String intToStr(int value) {
        return String.valueOf(value);
    }

    public static String intToStr(long value) {
        return String.valueOf(value);
    }

    // 兼容 delphi 代码
    public static String intToStr(double value) {
        return String.valueOf(value);
    }

    // 兼容 delphi 代码
    public static int strToIntDef(String str, int def) {
        int result;
        try {
            result = Integer.parseInt(str);
        } catch (Exception e) {
            result = def;
        }
        return result;
    }

    // 兼容 delphi 代码
    public static double strToDoubleDef(String str, double def) {
        double result;
        try {
            result = new BigDecimal(str).doubleValue();
        } catch (Exception e) {
            result = def;
        }
        return result;
    }

    // 兼容 delphi 代码
    public static String floatToStr(Double value) {
        return String.valueOf(value);
    }

    // 兼容 delphi 代码
    public static String newGuid() {
        UUID uuid = UUID.randomUUID();
        return '{' + uuid.toString() + '}';
    }

    /**
     * @return 生成全局唯一32位字符串
     */
    public static String getGuid() {
        String uuid = UUID.randomUUID().toString();
        return uuid.replaceAll("-", "");
    }

    @Deprecated
    public static String guidFixStr() {
        return getGuid();
    }

    @Deprecated
    public static String generateToken() {
        return getGuid();
    }

    // 兼容 delphi 代码
    public static String copy(String text, int iStart, int iLength) {
        if (text == null) {
            return "";
        }
        if (iLength >= text.length()) {
            if (iStart > text.length()) {
                return "";
            }
            if (iStart - 1 < 0) {
                return "";
            }
            return text.substring(iStart - 1);
        } else if ("".equals(text)) {
            return "";
        }
        return text.substring(iStart - 1, iStart - 1 + iLength);
    }

    // 兼容 delphi 代码
    public static String replace(String text, String sub, String rpl) {
        return text.replace(sub, rpl);
    }

    /**
     * <pre>
     * Utils.trim(null)          = null
     * Utils.trim("")            = ""
     * Utils.trim("     ")       = ""
     * Utils.trim("abc")         = "abc"
     * Utils.trim("    abc    ") = "abc"
     * </pre>
     *
     * @param str 目标字符串
     * @return 去除字符串前后的空格
     */
    public static String trim(String str) {
        return str == null ? null : str.trim();
    }

    /**
     * <pre>
     * Utils.trimToNull(null)          = null
     * Utils.trimToNull("")            = null
     * Utils.trimToNull("     ")       = null
     * Utils.trimToNull("abc")         = "abc"
     * Utils.trimToNull("    abc    ") = "abc"
     * </pre>
     *
     * @param str 目标字符串
     * @return 字符串为空(null)或者空串都转化为 null
     */
    public static String trimToNull(final String str) {
        final String ts = trim(str);
        return isEmpty(ts) ? null : ts;
    }

    /**
     * <pre>
     * Utils.trimToEmpty(null)          = ""
     * Utils.trimToEmpty("")            = ""
     * Utils.trimToEmpty("     ")       = ""
     * Utils.trimToEmpty("abc")         = "abc"
     * Utils.trimToEmpty("    abc    ") = "abc"
     * </pre>
     *
     * @param str 目标字符串
     * @return 字符串为空(null)或者空串都转化为空串
     */
    public static String trimToEmpty(final String str) {
        return str == null ? EMPTY : str.trim();
    }

    /**
     * 取得大于等于X的最小的整数，即：进一法
     *
     * @param val 参数
     * @return 大于等于Val最小的整数
     */
    public static int ceil(double val) {
        int result = (int) val;
        return (val > result) ? result + 1 : result;
    }

    /**
     * 取得X的整数部分，即：去尾法
     *
     * @param val 参数
     * @return 整数部分
     */
    public static double trunc(double val) {
        return (int) val;
    }

    /**
     * @param text 要检测的文本
     * @return 判断字符串是否全部为数字
     */
    public static boolean isNumeric(String text) {
        if (text == null)
            return false;
        if (".".equals(text))
            return false;
        return text.matches("[-+]?\\d+(?:\\.\\d+)?");
    }

    public static boolean isNotNumeric(String text) {
        return !Utils.isNumeric(text);
    }

    // 兼容 delphi 代码
    public static boolean assigned(Object object) {
        return object != null;
    }

    // 兼容 delphi 代码
    public static String isNull(String text, String def) {
        // 判断是否为空如果为空就返回。
        return "".equals(text) ? def : text;
    }

    /**
     * 若无特殊定制的情况，建议改为使用 Utils.roundTo() 方法
     */
    // 兼容 delphi 代码
    public static String formatFloat(String fmt, double value) {
        DecimalFormat df = new DecimalFormat(fmt);
        if (LanguageResource.isLanguageTW()) {
            df.setRoundingMode(RoundingMode.HALF_UP);
        }
        return df.format(new BigDecimal(Double.toString(value)));
    }

    /**
     * 创建指定长度的随机数
     *
     * @param len 长度
     * @return 随机数
     */
    public static String getNumRandom(int len) {
        SecureRandom random = new SecureRandom();
        StringBuilder verify = new StringBuilder();
        for (int i = 0; i < len; i++) {
            verify.append(random.nextInt(10));
        }
        return verify.toString();
    }

    /**
     * @param min 最小值
     * @param max 最大值
     * @return 获取指定范围内的随机整数
     */
    public static int random(int min, int max) {
        if (max < min) {
            throw new RuntimeException("max must > min");
        }
        SecureRandom random = new SecureRandom();
        return random.nextInt((max - min) + 1) + min;
    }

    // 转成指定类型的对象
    public static <T> T recordAsObject(DataRow record, Class<T> clazz) {
        T obj;
        try {
            obj = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e.getMessage());
        }
        for (Field method : clazz.getDeclaredFields()) {
            if (method.getAnnotation(Transient.class) != null) {
                continue;
            }
            Column column = method.getAnnotation(Column.class);
            String dbField = method.getName();
            String field = method.getName().substring(0, 1).toUpperCase() + method.getName().substring(1);
            if (column != null && !"".equals(column.name())) {
                dbField = column.name();
            }
            if (record.exists(dbField)) {
                try {
                    if (method.getType().equals(Integer.class)) {
                        Integer value = record.getInt(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else if (method.getType().equals(int.class)) {
                        int value = record.getInt(dbField);
                        Method set = clazz.getMethod("set" + field, int.class);
                        set.invoke(obj, value);
                    } else if ((method.getType().equals(Double.class))) {
                        Double value = record.getDouble(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else if ((method.getType().equals(double.class))) {
                        double value = record.getDouble(dbField);
                        Method set = clazz.getMethod("set" + field, double.class);
                        set.invoke(obj, value);
                    } else if ((method.getType().equals(Long.class))) {
                        Double value = record.getDouble(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else if ((method.getType().equals(long.class))) {
                        long value = (long) record.getDouble(dbField);
                        Method set = clazz.getMethod("set" + field, long.class);
                        set.invoke(obj, value);
                    } else if (method.getType().equals(Boolean.class)) {
                        Boolean value = record.getBoolean(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else if (method.getType().equals(boolean.class)) {
                        boolean value = record.getBoolean(dbField);
                        Method set = clazz.getMethod("set" + field, boolean.class);
                        set.invoke(obj, value);
//                    } else if (method.getType().equals(TDate.class)) {
//                        TDate value = record.getDate(dbField);
//                        Method set = clazz.getMethod("set" + field, value.getClass());
//                        set.invoke(obj, value);
//                    } else if (method.getType().equals(TDateTime.class)) {
//                        TDateTime value = record.getDateTime(dbField);
//                        Method set = clazz.getMethod("set" + field, value.getClass());
//                        set.invoke(obj, value);
                    } else if (method.getType().equals(Datetime.class)) {
                        Datetime value = record.getDatetime(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else if (method.getType().equals(String.class)) {
                        String value = record.getString(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    } else {
                        log.warn("field {}, other type {}", field, method.getType().getName());
                        String value = record.getString(dbField);
                        Method set = clazz.getMethod("set" + field, value.getClass());
                        set.invoke(obj, value);
                    }
                } catch (NoSuchMethodException | SecurityException | IllegalArgumentException
                        | InvocationTargetException | IllegalAccessException e) {
                    log.warn(e.getMessage(), e);
                }
            }
        }
        return obj;
    }

    @Deprecated
    public static <T> void objectAsRecord(DataRow record, T object) {
        Class<?> clazz = object.getClass();
        for (Field method : clazz.getDeclaredFields()) {
            if (method.getAnnotation(Transient.class) != null) {
                continue;
            }
            GeneratedValue generatedValue = method.getAnnotation(GeneratedValue.class);
            if (generatedValue != null && generatedValue.strategy().equals(GenerationType.IDENTITY)) {
                continue;
            }

            String field = method.getName();
            Column column = method.getAnnotation(Column.class);
            String dbField = field;
            if (column != null && !"".equals(column.name())) {
                dbField = column.name();
            }

            Method get;
            try {
                field = field.substring(0, 1).toUpperCase() + field.substring(1);
                get = clazz.getMethod("get" + field);
                Object value = get.invoke(object);
                record.setValue(dbField, value);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                // log.error(e.getMessage(), e);
            }
        }
    }

    // 将内容转成 Map
    public static <T> Map<String, T> dataSetAsMap(DataSet dataSet, Class<T> clazz, String... keys) {
        Map<String, T> items = new HashMap<>();
        for (DataRow rs : dataSet) {
            String key = "";
            for (String field : keys) {
                if ("".equals(key)) {
                    key = rs.getString(field);
                } else {
                    key += ";" + rs.getString(field);
                }
            }
            items.put(key, recordAsObject(rs, clazz));
        }
        return items;
    }

    // 将内容转成 List
    public static <T> List<T> dataSetAsList(DataSet dataSet, Class<T> clazz) {
        List<T> items = new ArrayList<>();
        for (DataRow rs : dataSet) {
            items.add(recordAsObject(rs, clazz));
        }
        return items;
    }

    /**
     * Utils.confused("13927470636", 2, 4) = 13*****0636
     *
     * @param value      手机号码
     * @param fromLength 起始显示位数
     * @param endLength  倒数显示位数
     * @return 混淆字符串指定位置
     */
    public static String confused(String value, int fromLength, int endLength) {
        if (Utils.isEmpty(value))
            return value;

        if (Math.min(fromLength, endLength) < 0)
            return confused(value);

        int length = value.length();
        if (length <= (fromLength + endLength))
            return confused(value);

        int len = length - fromLength - endLength;
        String star = "*".repeat(Math.max(0, len));// 需要复制的*个数
        return value.substring(0, fromLength) + star + value.substring(length - endLength);
    }

    /**
     * 脱敏字符串只保留头尾
     * 
     * Utils.confused("a") = "*"
     * 
     * Utils.confused("ab") = "**"
     * 
     * Utils.confused("abc") = "a*c"
     * 
     * Utils.confused("abcd") = "a**d"
     * 
     * @param value 原始字符串
     * @return 脱敏后字符串
     */
    public static String confused(String value) {
        if (Utils.isEmpty(value))
            return value;

        int len = (value.length() - (value.length() / 2)) / 2;
        return confused(value, len, len);
    }

    /**
     * 获取数字和字母的混合字符串
     *
     * @param length 长度
     * @return 混合字符串
     */
    public static String getStrRandom(int length) {
        StringBuilder result = new StringBuilder();
        SecureRandom random = new SecureRandom();
        for (int i = 0; i < length; i++) {
            String symbol = random.nextInt(2) % 2 == 0 ? "char" : "num";
            if ("char".equalsIgnoreCase(symbol)) {
                // 随机获取大小写字母
                int letterIndex = random.nextInt(2) % 2 == 0 ? 65 : 97;
                result.append((char) (random.nextInt(26) + letterIndex));
            } else {
                result.append(random.nextInt(10));
            }
        }
        return result.toString();
    }

    // 兼容 delphi 代码
    public static int random(int value) {
        return (int) (Math.random() * value);
    }

    // 兼容 delphi 代码
    public static String iif(boolean flag, String val1, String val2) {
        return flag ? val1 : val2;
    }

    // 兼容 delphi 代码
    public static double iif(boolean flag, double val1, double val2) {
        return flag ? val1 : val2;
    }

    // 兼容 delphi 代码
    public static int iif(boolean flag, int val1, int val2) {
        return flag ? val1 : val2;
    }

    /**
     * <pre>
     * Utils.isEmpty(null)      = true
     * Utils.isEmpty("")        = true
     * Utils.isEmpty(" ")       = false
     * Utils.isEmpty("bob")     = false
     * Utils.isEmpty("  bob  ") = false
     * </pre>
     *
     * @param str 目标字符串
     * @return 判断字符串是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 判断集合为空
     *
     * @param values 目标集合
     * @return 判断目标集合是否为空
     */
    public static boolean isEmpty(Collection<?> values) {
        return values == null || values.size() == 0;
    }

    /**
     * 判断集合是否为空
     *
     * @param values 目标数组
     * @return 判断目标数组是否为空
     */
    public static boolean isEmpty(Object[] values) {
        return values == null || values.length == 0;
    }

    /**
     * 判断Map为空
     *
     * @param map 目标Map
     * @return 判断目标Map是否为空，为NULL或键值对个数为0
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return map == null || map.size() == 0;
    }

    /**
     * <pre>
     * Utils.isNotEmpty(null)      = false
     * Utils.isNotEmpty("")        = false
     * Utils.isNotEmpty(" ")       = true
     * Utils.isNotEmpty("bob")     = true
     * Utils.isNotEmpty("  bob  ") = true
     * </pre>
     *
     * @param str 目标字符串
     * @return 判断字符串不为空
     */
    public static boolean isNotEmpty(String str) {
        return !Utils.isEmpty(str);
    }

    /**
     * <pre>
     * Utils.isBlank(null)      = true
     * Utils.isBlank("")        = true
     * Utils.isBlank(" ")       = true
     * Utils.isBlank("bob")     = false
     * Utils.isBlank("  bob  ") = false
     * </pre>
     *
     * @param str 目标字符串
     * @return 判断是否为纯空格
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((!Character.isWhitespace(str.charAt(i)))) {
                return false;
            }
        }
        return true;
    }

    /**
     * <pre>
     * StringUtils.isNotBlank(null)      = false
     * StringUtils.isNotBlank("")        = false
     * StringUtils.isNotBlank(" ")       = false
     * StringUtils.isNotBlank("bob")     = true
     * StringUtils.isNotBlank("  bob  ") = true
     * </pre>
     *
     * @param str 目标字符串
     * @return 判断是否不含纯空格
     */
    @Deprecated
    public static boolean isNotBlank(String str) {
        return !Utils.isBlank(str);
    }

    @Deprecated
    public static String findTable(Class<? extends EntityImpl> clazz) {
        return EntityHelper.get(clazz).tableName();
    }

    @Deprecated
    public static String findOid(Class<? extends EntityImpl> clazz, String defaultUid) {
        return EntityHelper.get(clazz).idFieldCode();
    }

    /**
     * 格式化输出JSON字符串
     */
    public static String formatJson(String json) {
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(jsonObject);
    }

    /**
     * 按数量对List进行分组
     *
     * @param sourceList 原始组List
     * @param groupSize  每组数量单位
     */
    public static <T> List<List<T>> divideList(List<T> sourceList, int groupSize) {
        List<List<T>> list = new ArrayList<>(groupSize);
        if (sourceList == null || sourceList.size() == 0)
            return list;
        if (groupSize <= 0)
            return list;

        // 先算出分组的数量再按照分组进行切割
        int listSize = sourceList.size();
        int num = (listSize + groupSize - 1) / groupSize;
        for (int i = 0; i < num; i++) {
            int startIndex = i * groupSize;
            int endIndex = Math.min(startIndex + groupSize, listSize);
            list.add(sourceList.subList(startIndex, endIndex));
        }
        return list;
    }

}