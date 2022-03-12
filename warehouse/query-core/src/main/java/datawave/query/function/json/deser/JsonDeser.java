package datawave.query.function.json.deser;

import com.google.gson.*;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

public class JsonDeser implements com.google.gson.JsonSerializer<Document>,com.google.gson.JsonDeserializer<Document>{


    /**
     * Add the raw attribute data
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttributeData(Attribute<?> attr,String name, JsonObject jsonObject) {
        jsonObject.addProperty(name,attr.toString());

    }

    /**
     * Add the metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttributeMetadata(Attribute<?> attr,String name, JsonObject jsonObject){
        if ( attr.isMetadataSet() ){
            Key metadata = attr.getMetadata();
            jsonObject.addProperty("row",metadata.getRow().toString());
            jsonObject.addProperty("cf",metadata.getColumnFamily().toString());
            jsonObject.addProperty("cq",metadata.getColumnQualifier().toString());
            jsonObject.addProperty("cv",metadata.getColumnVisibility().toString());
            jsonObject.addProperty("timestamp",metadata.getTimestamp());
        }
    }

    /**
     * Add the data and metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttribute(Attribute<?> attr,String name, JsonObject jsonObject) {
        addAttributeData(attr,name,jsonObject);
        addAttributeMetadata(attr,name,jsonObject);
    }


    /**
     * Typed conversion of the JSON array.
     * @param arrayAttr array attribute containing multiple Attribute instances.
     * @param name name of the Attributes collection
     * @param jsonDocument json document in which we emplace the new JsonArray.
     */
    private static void addJsonObject(Attribute<?> arrayAttr,String name, JsonArray jsonDocument){
        if (arrayAttr instanceof TypeAttribute && ((TypeAttribute)arrayAttr).getType() instanceof NumberType){

            JsonObject obj = new JsonObject();
            obj.addProperty(name,(BigDecimal)((TypeAttribute)arrayAttr).getType().denormalize());
            addAttributeMetadata(arrayAttr,name,obj);
            jsonDocument.add(obj);


        }
        else
        {
            JsonObject obj = new JsonObject();
            addAttribute(arrayAttr,name,obj);
            jsonDocument.add(obj);
        }

    }

    /**
     * Adds a new JsonElement to the jsonDocument.
     * @param attr attribute to convert
     * @param name name of the attribute
     * @param jsonDocument json document.
     */
    private static void addJsonObject(Attribute<?> attr,String name, JsonObject jsonDocument){
        if (attr instanceof Attributes){
            // we have an array
            JsonArray array = new JsonArray();
            for(Attribute<?> subA : ((Attributes)attr).getAttributes()){
                addJsonObject(subA,name,array);
            }
            jsonDocument.add(name,array);
        }
        if (attr instanceof TypeAttribute && ((TypeAttribute)attr).getType() instanceof NumberType){

            jsonDocument.addProperty(name,(BigDecimal)((TypeAttribute)attr).getType().denormalize());
            addAttributeMetadata(attr,name,jsonDocument);
        }
        else{
            addAttribute(attr,name,jsonDocument);
        }

    }

    /**
     * Serialize the document into a JsonDocument
     * @param document Datawave document
     * @param type type object
     * @param jsonSerializationContext serializer
     * @return JsonElement reflecting the new JsonDocument
     */
    public JsonElement serialize(Document document, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonDocument = new JsonObject();

        for(Map.Entry<String, Attribute<?>> entry : document.getDictionary().entrySet()){
            Attribute<?> attr = entry.getValue();
            addJsonObject(attr,entry.getKey(),jsonDocument);
        }



        return jsonDocument;
    }

    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     */
    private static TypeAttribute<?> elementToAttribute(JsonElement element){
        Key key = new Key();
        TypeAttribute<?> attr = null;
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()){
            NumberType type = new NumberType(element.getAsString());
            attr = new TypeAttribute<>(type,key,true);
        }
        else{
            NoOpType type = null;
            if (element instanceof JsonObject){
                JsonObject obj = (JsonObject)element;
                if ( obj.has("row") ){ // it has metadata
                    key = new Key(obj.get("row").getAsString(),obj.get("cf").getAsString(),obj.get("cq").getAsString(),obj.get("cv").getAsString(),obj.get("timestamp").getAsLong());
                }
                Map.Entry<String,JsonElement> ret = obj.entrySet().stream().filter( entry ->{
                    String str = entry.getKey();
                    return !str.equals("row") && !str.equals("cf") && !str.equals("cq") && !str.equals("cv") && !str.equals("timestamp");
                }).iterator().next();
                if (ret.getValue().isJsonPrimitive() && ret.getValue().getAsJsonPrimitive().isNumber()){
                    NumberType primitiveType = new NumberType(element.getAsString());
                    attr = new TypeAttribute<>(primitiveType,key,true);
                }
                else {
                    type = new NoOpType(ret.getValue().getAsString());
                    attr = new TypeAttribute<>(type, key, true);
                }
            }
            else {
                type = new NoOpType(element.getAsString());
                attr = new TypeAttribute<>(type, key, true);
            }

        }
        return attr;
    }

    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     * @param name name of the typed attribute
     * @param doc document to emplace the JsonElement attribute.
     */
    private static void populateAttribute(JsonElement element,String name, Document doc){
        doc.put(name,elementToAttribute(element));
    }

    /**
     * Populate an array of Attributes
     * @param array JsonArray
     * @param name name of the Attributes
     * @param doc document to emplace the attributes.
     */
    private static void populateAttributes(JsonArray array,String name, Document doc){
        final Attributes attrs = new Attributes(true);
        array.iterator().forEachRemaining( x -> {
            attrs.add(elementToAttribute(x));
        });
        doc.put(name,attrs);
    }
    @Override
    public Document deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        final Document doc = new Document();

        if (jsonElement instanceof JsonObject){
            ((JsonObject)jsonElement).entrySet().stream().forEach(
                    x->{ // Entry<String,JsonElement>
                        if (x.getValue() instanceof JsonArray){
                            // we have Attributes
                            populateAttributes((JsonArray)x.getValue(),x.getKey(),doc);
                        }
                        else{
                            populateAttribute(x.getValue(),x.getKey(),doc);
                        }
                    }
            );
        }

        return doc;
    }
}