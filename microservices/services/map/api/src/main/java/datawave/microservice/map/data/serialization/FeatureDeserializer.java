package datawave.microservice.map.data.serialization;

import java.io.IOException;

import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.Feature;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class FeatureDeserializer extends JsonDeserializer<Feature> {
    @Override
    public Feature deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        return new FeatureJSON().readFeature(jsonParser.getText().trim());
    }
}
