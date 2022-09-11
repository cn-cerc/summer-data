package cn.cerc.db.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import cn.cerc.db.Alias;

public class RecordImpl implements InvocationHandler {

    private DataRow dataRow;

    public RecordImpl(DataRow dataRow) {
        this.dataRow = dataRow;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        String field = method.getName();
        Alias alias = method.getAnnotation(Alias.class);
        if (alias != null && alias.value().length() > 0)
            field = alias.value();
        if (method.getReturnType() == Variant.class)
            result = new Variant(dataRow.getValue(field)).setTag(field);
        else if (method.getReturnType() == String.class)
            result = dataRow.getString(field);
        else if (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class)
            result = dataRow.getBoolean(field);
        else if (method.getReturnType() == int.class || method.getReturnType() == Integer.class)
            result = dataRow.getInt(field);
        else if (method.getReturnType() == double.class || method.getReturnType() == Double.class)
            result = dataRow.getDouble(field);
        else if (method.getReturnType() == long.class || method.getReturnType() == Long.class)
            result = dataRow.getLong(field);
        else if (method.getReturnType() == Datetime.class)
            result = dataRow.getDatetime(field);
        else
            result = dataRow.getValue(field);
        return result;
    }

}
