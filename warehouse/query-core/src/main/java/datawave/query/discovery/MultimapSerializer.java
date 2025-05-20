package datawave.query.discovery;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class MultimapSerializer implements JsonSerializer<Multimap<String,String>>, JsonDeserializer<Multimap<String,String>> {

    @Override
    public Multimap<String,String> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        ArrayListMultimap<String,String> mm = ArrayListMultimap.create();
        for (Entry<String,JsonElement> e : json.getAsJsonObject().entrySet()) {
            JsonArray values = e.getValue().getAsJsonArray();
            if (values.size() == 0) {
                mm.putAll(e.getKey(), Collections.emptyList());
            } else {
                for (JsonElement value : values)
                    mm.put(e.getKey(), value.getAsString());
            }
        }
        return mm;
    }

    @Override
    public JsonElement serialize(Multimap<String,String> src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject mm = new JsonObject();
        for (Entry<String,Collection<String>> e : src.asMap().entrySet()) {
            JsonArray values = new JsonArray();
            Collection<String> filtered = Collections2.filter(e.getValue(), Predicates.notNull());
            for (String value : filtered)
                values.add(new JsonPrimitive(value));
            mm.add(e.getKey(), values);
        }
        return mm;
    }

}
