package datawave.webservice.websocket.codec;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonReader;
import javax.json.stream.JsonParser;
import javax.websocket.DecodeException;
import javax.websocket.Decoder;
import javax.websocket.EndpointConfig;
import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import datawave.webservice.websocket.messages.CancelMessage;
import datawave.webservice.websocket.messages.CreateQueryMessage;
import datawave.webservice.websocket.messages.QueryMessage;

/**
 * Decodes incoming JSON text into a {@link QueryMessage}. Based on the message content, the returned object will be one of the known types of query messages.
 */
public class JsonQueryMessageDecoder implements Decoder.Text<QueryMessage> {
    @Override
    public QueryMessage decode(String s) throws DecodeException {
        MultivaluedMapImpl<String,String> map = new MultivaluedMapImpl<>();
        try (JsonParser parser = Json.createParser(new StringReader(s))) {
            while (parser.hasNext()) {
                if (parser.next() == JsonParser.Event.KEY_NAME) {
                    String key = parser.getString();
                    addValueToMap(key, parser, map);
                }
            }
        }
        if (map.size() == 1 && map.containsKey("cancel"))
            return new CancelMessage();
        else
            return new CreateQueryMessage(map);
    }

    private void addValueToMap(String key, JsonParser parser, MultivaluedMap<String,String> map) {
        boolean done = true; // By default we expect only a single value, but we could see an array.
        do {
            switch (parser.next()) {
                case VALUE_STRING:
                case VALUE_NUMBER:
                    map.add(key, parser.getString());
                    break;
                case VALUE_TRUE:
                    map.add(key, Boolean.TRUE.toString());
                    break;
                case VALUE_FALSE:
                    map.add(key, Boolean.FALSE.toString());
                    break;
                case VALUE_NULL:
                    map.add(key, null);
                    break;
                case START_ARRAY:
                    // If we hit an array, then mark done=false so we'll iterate through the do..while loop until we see END_ARRAY
                    done = false;
                    break;
                case END_ARRAY:
                    // We should only hit this after having hit a corresponding START_ARRAY. Indicate that we're done so we exit the do..while loop.
                    done = true;
                    break;
                case START_OBJECT:
                    throw new IllegalStateException("Cannot decode nested objects.");
            }
        } while (!done);
    }

    @Override
    public boolean willDecode(String s) {
        // See if it's valid JSON. If so, then we indicate we can read it.
        try (JsonReader reader = Json.createReader(new StringReader(s))) {
            reader.read();
            return true;
        } catch (JsonException | IllegalStateException e) {
            return false;
        }
    }

    @Override
    public void init(EndpointConfig config) {}

    @Override
    public void destroy() {}
}
