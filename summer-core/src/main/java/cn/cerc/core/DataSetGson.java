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
    private final T dataSet;

    public DataSetGson(T dataSet) {
        super();
        this.dataSet = dataSet;
    }

    @Override
    public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject root = new JsonObject();
        if (src.getState() != 0)
            root.addProperty("state", src.getState());
        if (src.getMessage() != null)
            root.addProperty("message", src.getMessage());

        // 输出metaInfo
        if (src.isMetaInfo()) {
            JsonObject meta = new JsonObject();
            if (src.getHead().getFieldDefs().size() > 0) {
                JsonArray head = new JsonArray();
                src.getHead().getFieldDefs().forEach(def -> head.add(context.serialize(def)));
                meta.add("head", head);
            }
            JsonArray body = new JsonArray();
            src.getFieldDefs().forEach(def -> body.add(context.serialize(def)));
            meta.add("body", body);
            root.add("meta", meta);
        }
        if (src.getHead().getFieldDefs().size() > 0) {
            if (src.isMetaInfo()) {
                JsonArray item = new JsonArray();
                src.getHead().getFieldDefs().forEach(def -> {
                    String field = def.getCode();
                    Object obj = src.getHead().getField(field);
                    item.add(context.serialize(obj));
                });
                root.add("head", item);
            } else {
                JsonObject item = new JsonObject();
                src.getHead().getFieldDefs().forEach(def -> {
                    String field = def.getCode();
                    Object obj = src.getHead().getField(field);
                    item.add(field, context.serialize(obj));
                });
                root.add("head", item);
            }
        }

        if (src.size() > 0) {
            JsonArray body = new JsonArray();
            // 添加字段定义
            if (!src.isMetaInfo())
                body.add(context.serialize(src.getFieldDefs()));
            src.getRecords().forEach(dataRow -> body.add(context.serialize(dataRow)));
            root.add("body", body);
        }

        return root;
    }

    @Override
    public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject root = json.getAsJsonObject();
        if (root.size() == 0)
            return dataSet;
        if (root.has("state"))
            dataSet.setState(root.get("state").getAsInt());
        if (root.has("message"))
            dataSet.setMessage(root.get("message").getAsString());

        if (root.has("meta")) {
            JsonObject meta = root.get("meta").getAsJsonObject();
            JsonArray head = meta.get("head").getAsJsonArray();
            head.forEach(item -> {
                FieldMeta def = context.deserialize(item, FieldMeta.class);
                dataSet.getHead().getFieldDefs().add(def);
            });
            JsonArray body = meta.get("body").getAsJsonArray();
            body.forEach(item -> {
                FieldMeta def = context.deserialize(item, FieldMeta.class);
                dataSet.getFieldDefs().add(def);
            });
            dataSet.setMetaInfo(true);
        }

        if (root.has("head")) {
            if (dataSet.isMetaInfo()) {
                JsonArray head = root.get("head").getAsJsonArray();
                int i = 0;
                for (String key : dataSet.getHead().getFieldDefs().getFields()) {
                    Object obj = context.deserialize(head.get(i++), Object.class);
                    dataSet.getHead().setField(key, obj);
                }
            } else {
                JsonObject head = root.get("head").getAsJsonObject();
                head.keySet().forEach(key -> {
                    Object obj = context.deserialize(head.get(key), Object.class);
                    dataSet.getHead().setField(key, obj);
                });
            }
        }

        JsonArray body = null;
        if (root.has("body"))
            body = root.get("body").getAsJsonArray();
        else if (root.has("dataset"))
            body = root.get("dataset").getAsJsonArray();
        if (body != null) {
            JsonArray defs = null;
            if (dataSet.isMetaInfo()) {
                defs = new JsonArray();
                for (FieldMeta meta : dataSet.getFieldDefs())
                    defs.add(meta.getCode());
            }
            for (int i = 0; i < body.size(); i++) {
                JsonArray item = body.get(i).getAsJsonArray();
                if (defs == null) {
                    item.forEach(field -> dataSet.getFieldDefs().add(field.getAsString()));
                    defs = item;
                } else {
                    DataRow current = dataSet.append().getCurrent();
                    for (int j = 0; j < defs.size(); j++) {
                        Object obj = context.deserialize(item.get(j), Object.class);
                        current.setField(defs.get(j).getAsString(), obj);
                    }
                }
            }
        }
        return dataSet;
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
                define.add(src.getType());
                if (src.getRemark() != null) {
                    define.add(src.getName());
                    define.add(src.getRemark());
                } else if (src.getName() != null) {
                    define.add(src.getName());
                }
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
                if (!def.get(0).isJsonNull())
                    meta.setType(def.get(0).getAsString());
                if (def.size() > 1)
                    meta.setName(def.get(1).getAsString());
                if (def.size() > 2)
                    meta.setRemark(def.get(2).getAsString());

                return meta;
            }

        };

        JsonSerializer<DataRow> gsonRecord = (src, typeOfSrc, context) -> {
            JsonArray item = new JsonArray();
            src.getFieldDefs().forEach(def -> {
                Object obj = src.getField(def.getCode());
                item.add(context.serialize(obj));
            });
            return item;
        };

        JsonSerializer<Date> gsonDate = (src, typeOfSrc,
                context) -> new JsonPrimitive((new Datetime(src.getTime())).toString());

        JsonSerializer<Double> gsonDouble = (src, typeOfSrc, context) -> {
            if (src == src.longValue())
                return new JsonPrimitive(src.longValue());
            return new JsonPrimitive(src);
        };

        GsonBuilder build = new GsonBuilder().registerTypeAdapter(FieldDefs.class, gsonFieldDefs)
                .registerTypeAdapter(FieldMeta.class, gsonFieldMeta).registerTypeAdapter(DataRow.class, gsonRecord)
                .registerTypeAdapter(Date.class, gsonDate).registerTypeAdapter(Double.class, gsonDouble)
                .serializeNulls();

        build.registerTypeAdapter(dataSet.getClass(), this);

        return build.create();
    }

    public final void decode(String json) {
        build().fromJson(json, dataSet.getClass());
    }

    public final String encode() {
        return build().toJson(dataSet);
    }

}
