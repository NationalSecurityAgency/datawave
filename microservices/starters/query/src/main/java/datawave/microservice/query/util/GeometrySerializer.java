package datawave.microservice.query.util;

import java.io.IOException;

import org.locationtech.jts.geom.Geometry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class GeometrySerializer extends JsonSerializer<Geometry> {
    @Override
    public void serialize(Geometry geometry, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeString(geometry.toText());
    }
}
