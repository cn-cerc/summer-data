package cn.cerc.core;

import java.lang.reflect.Type;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class DataSetGson<T extends DataSet> implements GsonInterface<T> {
    private T ds;

    public DataSetGson(T ds) {
        super();
        this.ds = ds;
    }

    @Override
    public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject root = new JsonObject();
        if (src.state != 0)
            root.addProperty("state", src.state);
        if (src.message != null)
            root.addProperty("message", src.message);

        // 输出metaInfo
        if (src.metaInfo) {
            JsonObject meta = new JsonObject();
            if (src.head != null && src.head.getFieldDefs().size() > 0) {
                JsonArray head = new JsonArray();
                src.head.getFieldDefs().forEach(def -> head.add(context.serialize(def)));
                meta.add("head", head);
            }
            JsonArray body = new JsonArray();
            src.getFieldDefs().forEach(def -> body.add(context.serialize(def)));
            meta.add("body", body);
            root.add("meta", meta);
        }

        if (src.head != null && src.head.size() > 0) {
            JsonObject item = new JsonObject();
            src.head.getFieldDefs().forEach(def -> {
                String field = def.getCode();
                Object obj = src.head.getField(field);
                if (obj == null)
                    item.addProperty(field, "{}");
                else
                    item.add(field, context.serialize(obj));
            });
            root.add("head", item);
        }

        if (src.size() > 0) {
            JsonArray body = new JsonArray();
            // 添加字段定义
            body.add(context.serialize(src.getFieldDefs()));
            src.records.forEach(dataRow -> body.add(context.serialize(dataRow)));
            root.add("body", body);
        }

        return root;
    }

    @Override
    public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject root = json.getAsJsonObject();
        if (root.size() == 0)
            return ds;
        if (root.has("state"))
            ds.state = root.get("state").getAsInt();
        if (root.has("message"))
            ds.message = root.get("message").getAsString();

        if (root.has("meta")) {
            JsonObject meta = root.get("meta").getAsJsonObject();
            JsonArray head = meta.get("head").getAsJsonArray();
            head.forEach(item -> {
                FieldMeta def = context.deserialize(item, FieldMeta.class);
                ds.getHead().getFieldDefs().add(def);
            });
            JsonArray body = meta.get("body").getAsJsonArray();
            body.forEach(item -> {
                FieldMeta def = context.deserialize(item, FieldMeta.class);
                ds.getFieldDefs().add(def);
            });
            ds.metaInfo = true;
        }

        if (root.has("head")) {
            JsonObject head = root.get("head").getAsJsonObject();
            head.keySet().forEach(key -> {
                Object obj = context.deserialize(head.get(key), Object.class);
                ds.getHead().setField(key, obj);
            });
        }

        JsonArray body = null;
        if (root.has("body"))
            body = root.get("body").getAsJsonArray();
        else if (root.has("dataset"))
            body = root.get("dataset").getAsJsonArray();
        if (body != null) {
            JsonArray defs = null;
            for (int i = 0; i < body.size(); i++) {
                JsonArray item = body.get(i).getAsJsonArray();
                if (i == 0) {
                    item.forEach(field -> ds.getFieldDefs().add(field.getAsString()));
                    defs = item;
                } else {
                    Record current = ds.append().getCurrent();
                    for (int j = 0; j < defs.size(); j++) {
                        Object obj = context.deserialize(item.get(j), Object.class);
                        current.setField(defs.get(j).getAsString(), obj);
                    }
                }
            }
        }
        return ds;
    }

    private Gson build() {
        JsonSerializer<FieldDefs> gsonFieldDefs = (src, typeOfSrc, context) -> {
            JsonArray root = new JsonArray();
            src.forEach(item -> root.add(item.getCode()));
            return root;
        };

        GsonInterface<FieldMeta> gsonFieldMeta = new GsonInterface<FieldMeta>() {
            @Override
            public JsonElement serialize(FieldMeta src, Type typeOfSrc, JsonSerializationContext context) {
                JsonObject json = new JsonObject();
                JsonArray define = new JsonArray();
                define.add(src.getName());
                define.add(src.getType());
                if (src.getRemark() != null)
                    define.add(src.getRemark());
                json.add(src.getCode(), define);
                return json;
            }

            @Override
            public FieldMeta deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                    throws JsonParseException {
                JsonObject root = json.getAsJsonObject();

                String[] list = root.keySet().toArray(new String[0]);
                String field = list[0];

                JsonArray def = root.get(field).getAsJsonArray();
                FieldMeta meta = new FieldMeta(field);
                meta.setName(def.get(0).getAsString());
                meta.setType(def.get(1).getAsString());
                if (def.size() > 2)
                    meta.setRemark(def.get(2).getAsString());

                return meta;
            }

        };

        JsonSerializer<Record> gsonRecord = (src, typeOfSrc, context) -> {
            JsonArray item = new JsonArray();
            src.getFieldDefs().forEach(def -> {
                Object obj = src.getField(def.getCode());
                if (obj == null) {
                    item.add("{}");
                } else {
                    item.add(context.serialize(obj));
                }
            });
            return item;
        };

        JsonSerializer<TDateTime> gsonTDateTime = (src, typeOfSrc, context) -> new JsonPrimitive(src.toString());

        JsonSerializer<Date> gsonDate = (src, typeOfSrc, context) -> new JsonPrimitive((new TDateTime(src)).toString());

        JsonSerializer<Double> gsonDouble = (src, typeOfSrc, context) -> {
            if (src == src.longValue())
                return new JsonPrimitive(src.longValue());
            return new JsonPrimitive(src);
        };

        GsonBuilder build = new GsonBuilder().registerTypeAdapter(FieldDefs.class, gsonFieldDefs)
                .registerTypeAdapter(FieldMeta.class, gsonFieldMeta).registerTypeAdapter(Record.class, gsonRecord)
                .registerTypeAdapter(TDateTime.class, gsonTDateTime).registerTypeAdapter(Date.class, gsonDate)
                .registerTypeAdapter(Double.class, gsonDouble).serializeNulls();

        build.registerTypeAdapter(ds.getClass(), this);

        return build.create();
    }

    public final void decode(String json) {
        build().fromJson(json, ds.getClass());
    }

    public final String encode() {
        return build().toJson(ds);
    }
}
