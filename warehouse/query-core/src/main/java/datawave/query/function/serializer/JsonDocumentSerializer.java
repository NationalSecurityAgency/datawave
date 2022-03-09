package datawave.query.function.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import datawave.query.attributes.Document;
import datawave.query.function.json.deser.JsonDeser;
import org.apache.log4j.Logger;

import java.io.StringWriter;

/**
 * Json Document Serializer converts the POJO using gsonBuilder to a json string.
 */
public class JsonDocumentSerializer extends DocumentSerializer {

    private static final Logger log = Logger.getLogger(JsonDocumentSerializer.class);

    static GsonBuilder gsonBuilder = new GsonBuilder();

    static{
        gsonBuilder.registerTypeAdapter(Document.class,new JsonDeser());
    }

    public JsonDocumentSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }

    @Override
    public byte[] serialize(Document doc) {
        log.warn("will serialize "+doc.toString());
        final Gson gson = gsonBuilder.create();
        final StringWriter writer = new StringWriter();
        gson.toJson(doc,writer);
        log.warn("serialized to "+writer.toString());
        return writer.toString().getBytes();
    }


}
