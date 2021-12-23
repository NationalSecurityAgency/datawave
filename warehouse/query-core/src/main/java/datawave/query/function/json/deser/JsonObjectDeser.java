package datawave.query.function.json.deser;


import com.google.gson.JsonObject;
import datawave.query.attributes.Attribute;
import org.apache.log4j.Logger;

/**
 * Supports JSON object serialization and deserialization to and fro Documents.
 *
 */
public class JsonObjectDeser extends JsonDeser{
    private static final Logger log = Logger.getLogger(JsonObjectDeser.class);

    private static class Holder {
        private static final JsonObjectDeser INSTANCE = new JsonObjectDeser();
    }

    public static JsonObjectDeser getInstance(){
        return Holder.INSTANCE;
    }

    private JsonObjectDeser(){
        super();
    }



    @Override
    /**
     * Add the raw attribute data
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    protected void addAttributeData(Attribute<?> attr,String name, JsonObject jsonObject) {
        jsonObject.addProperty(TYPE_DATA,attr.getData().toString());

    }



}