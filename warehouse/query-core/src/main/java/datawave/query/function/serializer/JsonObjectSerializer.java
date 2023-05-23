package datawave.query.function.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import datawave.query.attributes.Document;
import datawave.query.function.json.deser.JsonObjectDeser;
import java.io.StringWriter;

/**
 * Json Document Serializer converts the POJO using gsonBuilder to a json string.
 */
public class JsonObjectSerializer extends JsonMetadataSerializer {

    static GsonBuilder gsonBuilder = new GsonBuilder();
    static Gson gson;
    static{
        gsonBuilder.registerTypeAdapter(Document.class,JsonObjectDeser.getInstance());
        gson = gsonBuilder.create();
    }

    public JsonObjectSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }
    
    @Override
    public byte[] serialize(Document doc) {

        final StringWriter writer = new StringWriter();
        gson.toJson(doc,writer);
        return writer.toString().getBytes();
    }


}
