package datawave.microservice.map.data.serialization;

import java.io.IOException;
import java.io.StringWriter;

import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.GeoJSON;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;

public class FeatureCollectionSerializer extends JsonSerializer<FeatureCollection<?,?>> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    @Override
    public void serialize(FeatureCollection<?,?> featureCollection, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        StringWriter writer = new StringWriter();
        GeoJSON.write(featureCollection, writer);
        String unformattedJson = writer.toString();
        
        // if indent output is enabled, convert to json pojo so that the output can be properly indented
        if (serializerProvider.getConfig().isEnabled(SerializationFeature.INDENT_OUTPUT)) {
            try {
                jsonGenerator.writeObject(new JSONParser().parse(unformattedJson));
            } catch (ParseException e) {
                // if we fail to parse, log an error and write the raw json
                log.error("Parse exception reading unformatted Feature json", e);
                jsonGenerator.writeRawValue(unformattedJson);
            }
        }
        // otherwise, just write the raw string
        else {
            jsonGenerator.writeRawValue(unformattedJson);
        }
    }
}
