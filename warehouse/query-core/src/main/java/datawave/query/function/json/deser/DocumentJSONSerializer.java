package datawave.query.function.json.deser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

public class DocumentJSONSerializer extends StdSerializer<Document> {


    public DocumentJSONSerializer(Class<Document> t) {
        super(t);
    }

    public DocumentJSONSerializer(){
        this(null);
    }

    /**
     * Add the raw attribute data
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttributeData(Attribute<?> attr,String name, JsonGenerator jsonObject) throws IOException {
        if (attr instanceof TypeAttribute){
            datawave.data.type.Type t = ((TypeAttribute)attr).getType();
             if (t.getClass() != NoOpType.class)
                jsonObject.writeStringField("type.metadata",t.getClass().getCanonicalName());
        }


        jsonObject.writeStringField("type.data",attr.getData().toString());

    }

    private static void addAttributeData(TypeAttribute<?> attr,String name, JsonGenerator jsonObject) throws IOException {
        if (attr.getType().getClass() != NoOpType.class)
        jsonObject.writeStringField("type.metadata",attr.getType().getClass().getCanonicalName());
        jsonObject.writeStringField("type.data",attr.getData().toString());

    }

    /**
     * Add the metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttributeMetadata(Attribute<?> attr,String name, JsonGenerator jsonObject){
       /* if ( attr.isMetadataSet() ){
            Key metadata = attr.getMetadata();
            JsonObject key = new JsonObject();
            key.addProperty("row",metadata.getRow().toString());
            key.addProperty("cf",metadata.getColumnFamily().toString());
            key.addProperty("cq",metadata.getColumnQualifier().toString());
            key.addProperty("cv",metadata.getColumnVisibility().toString());
            key.addProperty("timestamp",metadata.getTimestamp());
            jsonObject.add("key",key);

        }*/
    }

    /**
     * Add the data and metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttribute(Attribute<?> attr,String name, JsonGenerator jsonObject) throws IOException {
        addAttributeData(attr,name,jsonObject);
        addAttributeMetadata(attr,name,jsonObject);
    }

    /**
     * Adds a new JsonElement to the jsonDocument.
     * @param attr attribute to convert
     * @param name name of the attribute
     * @param jsonDocument json document.
     */
    private static void addJsonObject(Attribute<?> attr,String name, JsonGenerator jsonDocument) throws IOException {
        if (attr instanceof Attributes){
            // we have an array

            jsonDocument.writeArrayFieldStart(name);
            for(Attribute<?> subA : ((Attributes)attr).getAttributes()){
                addJsonObject(subA,name,jsonDocument);
            }
            jsonDocument.writeEndArray();
        }
        else if (attr instanceof TypeAttribute && ((TypeAttribute)attr).getType() instanceof NumberType){

            jsonDocument.writeNumberField(name,((BigDecimal)((TypeAttribute)attr).getType().denormalize()).longValue());
            addAttributeMetadata(attr,name,jsonDocument);
        }
        else{
            jsonDocument.writeObjectFieldStart(name);
            addAttribute(attr,name,jsonDocument);
            jsonDocument.writeEndObject();
        }

    }
    @Override
    public void serialize(Document document, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        for(Map.Entry<String, Attribute<?>> entry : document.getDictionary().entrySet()){
            Attribute<?> attr = entry.getValue();
            addJsonObject(attr,entry.getKey(),jsonGenerator);
        }
        if (document.isMetadataSet()){
            addAttributeMetadata(document,"key",jsonGenerator);
        }
        jsonGenerator.writeEndObject();
    }
}
