package datawave.webservice.common.json;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Deserializes JSON received in a POST parameter into a MultivaluedMap&lt;String, String&gt; */
public class MultivaluedMapDeserializer extends JsonDeserializer<MultivaluedMap<String,String>> {
    @Override
    public MultivaluedMap<String,String> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException, JsonProcessingException {
        final MultivaluedMap<String,String> result = new MultivaluedHashMap<>();
        ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        JsonNode node = mapper.readTree(jsonParser);

        if (!node.isObject()) {
            throw new JsonParseException(jsonParser, "Expected an object, but received a " + node.getNodeType());
        }

        final Iterator<Map.Entry<String,JsonNode>> it = node.fields();

        while (it.hasNext()) {
            final Map.Entry<String,JsonNode> entry = it.next();
            final JsonNode value = entry.getValue();
            if (!value.isTextual()) {
                throw new JsonParseException(jsonParser, "Expected a textual value, but received a " + value.getNodeType());
            }
            result.add(entry.getKey(), value.asText());
        }

        return result;
    }
}
