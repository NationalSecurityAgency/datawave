package datawave.query.discovery;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import datawave.core.query.jexl.LiteralRange;

public class LiteralRangeSerializer implements JsonSerializer<LiteralRange<String>>, JsonDeserializer<LiteralRange<String>> {

    @Override
    public LiteralRange<String> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = json.getAsJsonObject();
        String fieldName = (obj.get("FN") == null ? null : obj.get("FN").getAsString());
        String lower = (obj.get("L") == null ? null : obj.get("L").getAsString());
        String upper = (obj.get("U") == null ? null : obj.get("U").getAsString());
        LiteralRange.NodeOperand operand = (obj.get("O") == null ? null : LiteralRange.NodeOperand.valueOf(obj.get("O").getAsString()));
        Boolean lowerInclusive = (obj.get("LI") == null ? null : obj.get("LI").getAsBoolean());
        Boolean upperInclusive = (obj.get("UI") == null ? null : obj.get("UI").getAsBoolean());

        return new LiteralRange<>(lower, lowerInclusive, upper, upperInclusive, fieldName, operand);
    }

    @Override
    public JsonElement serialize(LiteralRange<String> src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject mm = new JsonObject();
        if (src.getFieldName() != null)
            mm.add("FN", new JsonPrimitive(src.getFieldName()));
        if (src.getLower() != null)
            mm.add("L", new JsonPrimitive(src.getLower()));
        if (src.getUpper() != null)
            mm.add("U", new JsonPrimitive(src.getUpper()));
        if (src.getNodeOperand() != null)
            mm.add("O", new JsonPrimitive(src.getNodeOperand().name()));
        if (src.isLowerInclusive() != null)
            mm.add("LI", new JsonPrimitive(src.isLowerInclusive()));
        if (src.isUpperInclusive() != null)
            mm.add("UI", new JsonPrimitive(src.isUpperInclusive()));
        return mm;
    }

}
