package datawave.query.discovery;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import datawave.query.jexl.LiteralRange;

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
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class LiteralRangeMultimapSerializer
                implements JsonSerializer<Multimap<String,LiteralRange<String>>>, JsonDeserializer<Multimap<String,LiteralRange<String>>> {

    private final LiteralRangeSerializer lrSerializer = new LiteralRangeSerializer();

    @Override
    public Multimap<String,LiteralRange<String>> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        ArrayListMultimap<String,LiteralRange<String>> mm = ArrayListMultimap.create();
        for (Entry<String,JsonElement> e : json.getAsJsonObject().entrySet()) {
            JsonArray values = e.getValue().getAsJsonArray();
            if (values.size() == 0) {
                mm.putAll(e.getKey(), Collections.emptyList());
            } else {
                for (JsonElement value : values)
                    mm.put(e.getKey(), lrSerializer.deserialize(value, typeOfT, context));
            }
        }
        return mm;
    }

    @Override
    public JsonElement serialize(Multimap<String,LiteralRange<String>> src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject mm = new JsonObject();
        for (Entry<String,Collection<LiteralRange<String>>> e : src.asMap().entrySet()) {
            JsonArray values = new JsonArray();
            Collection<LiteralRange<String>> filtered = Collections2.filter(e.getValue(), Predicates.notNull());
            for (LiteralRange<String> value : filtered)
                values.add(lrSerializer.serialize(value, typeOfSrc, context));
            mm.add(e.getKey(), values);
        }
        return mm;
    }

}
