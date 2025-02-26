package datawave.microservice.query.util;

import java.io.IOException;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class GeometryDeserializer extends JsonDeserializer<Geometry> {
    private WKTReader wktReader = new WKTReader();
    
    @Override
    public Geometry deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        try {
            return wktReader.read(jsonParser.getValueAsString());
        } catch (ParseException e) {
            throw new IOException("Failed to deserialize wkt to Geometry: " + jsonParser.getValueAsString(), e);
        }
    }
}
