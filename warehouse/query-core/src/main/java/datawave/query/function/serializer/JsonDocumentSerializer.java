package datawave.query.function.serializer;

import com.google.gson.*;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.json.deser.JsonDeser;

import java.io.StringWriter;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Json Document Serializer converts the POJO using gsonBuilder to a json string.
 */
public class JsonDocumentSerializer extends DocumentSerializer {

    static GsonBuilder gsonBuilder = new GsonBuilder();
    static Gson gson;
    static{
        gsonBuilder.registerTypeAdapter(Document.class,new JsonDeser());
        gson = gsonBuilder.create();
    }

    public JsonDocumentSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }

    @Override
    public byte[] serialize(Document doc) {

        final StringWriter writer = new StringWriter();
        gson.toJson(doc,writer);
        return writer.toString().getBytes();
    }


}
