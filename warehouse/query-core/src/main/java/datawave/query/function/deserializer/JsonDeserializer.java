package datawave.query.function.deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import datawave.query.attributes.Document;
import datawave.query.function.json.deser.JsonDeser;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Json Document Deserializer leveraging JsonDeser to convert the object from JSON to a
 * typed Document Object.
 */
public class JsonDeserializer extends DocumentDeserializer{

    static GsonBuilder gsonBuilder = new GsonBuilder();

    static{
        gsonBuilder.registerTypeAdapter(Document.class,new JsonDeser());
    }


    public JsonDeserializer(){
    }
    @Override
    public Document deserialize(final InputStream inputStream) {
        final Gson gson= gsonBuilder.create();
        try {
            String ss = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            Document d = gson.fromJson(ss,Document.class);
            return d;
        } catch (IOException e) {
            throw new RuntimeException("Could not convert Document through write().", e);
        }
    }
}


