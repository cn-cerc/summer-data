package cn.cerc.core;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;

public interface GsonInterface<T> extends JsonSerializer<T>, JsonDeserializer<T> {

}
