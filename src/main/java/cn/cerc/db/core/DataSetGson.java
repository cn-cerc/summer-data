package cn.cerc.db.core;

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
    private static final String CURD_STATE = "_state_";
    private final T dataSet;

    public DataSetGson(T dataSet) {
        super();
        this.dataSet = dataSet;
    }

    @Override
    public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject root = new JsonObject();
        if (src.state() != 0)
            root.addProperty("state", src.state());
        if (src.message() != null)
            root.addProperty("message", src.message());

        // 输出metaInfo
        if (src.meta()) {
            JsonObject meta = new JsonObject();
            if (src.head().fields().size() > 0) {
                JsonArray head = new JsonArray();
                src.head().fields().forEach(def -> head.add(context.serialize(def)));
                meta.add("head", head);
            }
            JsonArray body = new JsonArray();
            src.fields().forEach(def -> body.add(context.serialize(def)));
            if (src.crud())
                body.add(context.serialize(new FieldMeta(CURD_STATE)));
            meta.add("body", body);
            root.add("meta", meta);
        }
        if (src.head().fields().size() > 0) {
            if (src.meta()) {
                JsonArray item = new JsonArray();
                src.head().fields().forEach(def -> {
                    String field = def.code();
                    Object obj = src.head().getValue(field);
                    item.add(context.serialize(obj));
                });
                root.add("head", item);
            } else {
                JsonObject item = new JsonObject();
                src.head().fields().forEach(def -> {
                    String field = def.code();
                    Object obj = src.head().getValue(field);
                    item.add(field, context.serialize(obj));
                });
                root.add("head", item);
            }
        }

        JsonArray body = new JsonArray();
        // 添加字段定义
        if (!src.meta()) {
            body.add(context.serialize(src.fields()));
            if (src.crud()) {
                JsonElement item = body.get(body.size() - 1);
                item.getAsJsonArray().add(CURD_STATE);
            }
        }
        if (src.crud()) {
            // insert && update
            src.records().forEach(dataRow -> {
                if (dataRow.state() == DataRowState.Insert) {
                    body.add(context.serialize(dataRow));
                } else if (dataRow.state() == DataRowState.Update) {
                    body.add(context.serialize(dataRow.history()));
                    body.add(context.serialize(dataRow));
                }
            });
            // delete
            src.garbage().forEach(dataRow -> body.add(context.serialize(dataRow)));
        } else if (src.size() > 0) {
            src.records().forEach(dataRow -> body.add(context.serialize(dataRow)));
        }
        if (src.size() > 0)
            root.add("body", body);

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
            if (meta.has("head")) {
                JsonArray head = meta.get("head").getAsJsonArray();
                head.forEach(item -> {
                    FieldMeta def = context.deserialize(item, FieldMeta.class);
                    dataSet.head().fields().add(def);
                });
            }
            if (meta.has("body")) {
                JsonArray body = meta.get("body").getAsJsonArray();
                body.forEach(item -> {
                    FieldMeta def = context.deserialize(item, FieldMeta.class);
                    if (CURD_STATE.equals(def.code()))
                        dataSet.setCrud(true);
                    else
                        dataSet.fields().add(def);
                });
            }
            dataSet.setMeta(true);
        }

        if (root.has("head")) {
            if (dataSet.meta()) {
                JsonArray head = root.get("head").getAsJsonArray();
                int i = 0;
                for (String key : dataSet.head().fields().names()) {
                    Object obj = context.deserialize(head.get(i++), Object.class);
                    dataSet.head().setValue(key, obj);
                }
            } else {
                JsonObject head = root.get("head").getAsJsonObject();
                head.keySet().forEach(key -> {
                    Object value = context.deserialize(head.get(key), Object.class);
                    if (value instanceof Double) {
                        Double tmp = (Double) value;
                        if (tmp.toString().endsWith(".0") && tmp >= Integer.MIN_VALUE && tmp <= Integer.MAX_VALUE)
                            value = tmp.intValue();
                    }
                    dataSet.head().setValue(key, value);
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
            if (dataSet.meta()) {
                defs = new JsonArray();
                for (FieldMeta meta : dataSet.fields())
                    defs.add(meta.code());
                if (dataSet.crud())
                    defs.add(CURD_STATE);
            }

            DataRow history = null;
            for (int i = 0; i < body.size(); i++) {
                JsonArray item = body.get(i).getAsJsonArray();
                if (defs == null) {
                    item.forEach(field -> {
                        if (CURD_STATE.equals(field.getAsString()))
                            dataSet.setCrud(true);
                        else
                            dataSet.fields().add(field.getAsString());
                    });
                    defs = item;
                    continue;
                }
                //
                DataRow current = new DataRow(dataSet);
                for (int j = 0; j < defs.size(); j++) {
                    Object value = context.deserialize(item.get(j), Object.class);
                    if (value instanceof Double) {
                        Double tmp = (Double) value;
                        if (tmp.toString().endsWith(".0") && tmp >= Integer.MIN_VALUE && tmp <= Integer.MAX_VALUE)
                            value = tmp.intValue();
                    }
                    String field = defs.get(j).getAsString();
                    if (CURD_STATE.equals(field)) {
                        if (!(value instanceof Integer))
                            throw new RuntimeException(value.getClass().getName());
                        DataRowState state = null;
                        switch ((Integer) value) {
                        case 0:
                            state = DataRowState.None;
                            break;
                        case 1:
                            state = DataRowState.Insert;
                            break;
                        case 2:
                            state = DataRowState.Update;
                            break;
                        case 3:
                            state = DataRowState.Delete;
                            break;
                        case 4:
                            state = DataRowState.History;
                            break;
                        default:
                            throw new RuntimeException("error state value: " + value);
                        }
                        current.setState(state);
                    } else
                        current.setValue(field, value);
                }
                // 加入到数据集
                if (current.state() == DataRowState.History) {
                    if (history != null)
                        throw new RuntimeException("history is not null");
                    history = current;
                } else if (current.state() == DataRowState.Update) {
                    if (history == null)
                        throw new RuntimeException("history is null");
                    current.setHistory(history);
                    dataSet.records().add(current);
                    history = null;
                } else if (current.state() == DataRowState.Delete)
                    dataSet.garbage().add(current);
                else
                    dataSet.records().add(current);
            }
        }
        return dataSet;
    }

    private Gson build() {
        JsonSerializer<FieldDefs> gsonFieldDefs = (src, typeOfSrc, context) -> {
            JsonArray root = new JsonArray();
            src.forEach(item -> root.add(item.code()));
            return root;
        };

        GsonInterface<FieldMeta> gsonFieldMeta = new GsonInterface<FieldMeta>() {
            @Override
            public JsonElement serialize(FieldMeta src, Type typeOfSrc, JsonSerializationContext context) {
                JsonObject json = new JsonObject();
                JsonArray define = new JsonArray();
                if (src.remark() != null) {
                    define.add(src.name());
                    define.add(src.typeValue());
                    define.add(src.remark());
                } else if (src.typeValue() != null) {
                    define.add(src.name());
                    define.add(src.typeValue());
                } else if (src.name() != null) {
                    define.add(src.name());
                }
                json.add(src.code(), define);
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
                if (def.size() > 0) {
                    if (!def.get(0).isJsonNull())
                        meta.setName(def.get(0).getAsString());
                }
                if (def.size() > 1) {
                    if (!def.get(1).isJsonNull())
                        meta.dataType().setValue(def.get(1).getAsString());
                }
                if (def.size() > 2) {
                    if (!def.get(2).isJsonNull())
                        meta.setRemark(def.get(2).getAsString());
                }
                return meta;
            }

        };

        JsonSerializer<DataRow> gsonRecord = (src, typeOfSrc, context) -> {
            JsonArray item = new JsonArray();
            src.fields().forEach(def -> {
                Object obj = src.getValue(def.code());
                item.add(context.serialize(obj));
            });
            if (src.dataSet() != null && src.dataSet().crud())
                item.add(context.serialize(src.state().ordinal()));
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
