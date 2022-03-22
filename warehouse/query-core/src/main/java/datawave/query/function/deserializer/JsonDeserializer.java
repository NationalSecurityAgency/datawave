package datawave.query.function.deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import datawave.query.attributes.Document;
import datawave.query.function.json.deser.JsonDeser;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Json Document Deserializer leveraging JsonDeser to convert the object from JSON to a
 * typed Document Object.
 */
public class JsonDeserializer extends DocumentDeserializer{

    static GsonBuilder gsonBuilder = new GsonBuilder();
    static Gson gson;
    static{
        gsonBuilder.registerTypeAdapter(Document.class,new JsonDeser());
        gson= gsonBuilder.create();
    }


    public JsonDeserializer(){
    }
    @Override
    public Document deserialize(final InputStream inputStream) {

        return gson.fromJson(new InputStreamReader(inputStream),Document.class);
    }
}


