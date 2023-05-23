package datawave.query.function.deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import datawave.query.attributes.Document;
import datawave.query.function.json.deser.JsonDeser;

import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Json Document Deserializer leveraging JsonDeser to convert the object from JSON to a
 * typed Document Object.
 */
public class DocumentJsonDeserializer extends DocumentDeserializer{
    static GsonBuilder gsonBuilder = new GsonBuilder();
    static Gson gson;
    static{
        gsonBuilder.registerTypeAdapter(Document.class,JsonDeser.getInstance());
        gson= gsonBuilder.create();
    }
    public DocumentJsonDeserializer(){
    }
    @Override
    public Document deserialize(final InputStream inputStream) {
        return gson.fromJson(new InputStreamReader(inputStream),Document.class);
    }
}


