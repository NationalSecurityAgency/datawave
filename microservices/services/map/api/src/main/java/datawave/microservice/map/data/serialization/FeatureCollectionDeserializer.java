package datawave.microservice.map.data.serialization;

import java.io.IOException;

import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class FeatureCollectionDeserializer extends JsonDeserializer<FeatureCollection<?,?>> {
    @Override
    public FeatureCollection<?,?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        return new FeatureJSON().readFeatureCollection(jsonParser.getText().trim());
    }
}
