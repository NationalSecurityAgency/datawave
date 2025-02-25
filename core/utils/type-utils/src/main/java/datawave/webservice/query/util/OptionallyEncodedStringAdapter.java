package datawave.webservice.query.util;

import java.io.IOException;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * An {@link XmlAdapter} that allows a {@link String} property to be bound to XML that is encoded as an {@link OptionallyEncodedStringAdapter}.
 * 
 * @see OptionallyEncodedStringAdapter
 */
public class OptionallyEncodedStringAdapter extends XmlAdapter<OptionallyEncodedString,String> {
    
    @Override
    public String unmarshal(OptionallyEncodedString v) throws Exception {
        return v.getValue();
    }
    
    @Override
    public OptionallyEncodedString marshal(String v) throws Exception {
        return (v == null) ? null : new OptionallyEncodedString(v);
    }
    
    public static class Serializer extends JsonSerializer<Object> {
        @Override
        public void serialize(Object obj, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            // depending on how we get here, we may have the underlying string of an OptionallyEncodedString. Check for both.
            if (obj instanceof OptionallyEncodedString) {
                jsonGenerator.writeObject((OptionallyEncodedString) obj);
            } else {
                jsonGenerator.writeObject(new OptionallyEncodedString(String.valueOf(obj)));
            }
        }
    }
    
    public static class Deserializer extends JsonDeserializer<String> {
        
        @Override
        public String deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return jsonParser.readValueAs(OptionallyEncodedString.class).getValue();
        }
    }
    
}
