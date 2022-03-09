package datawave.query.function.json.deser;

import com.google.gson.*;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

public class JsonDeser implements com.google.gson.JsonSerializer<Document>,com.google.gson.JsonDeserializer<Document>{

    /**
     * Typed conversion of the JSON array.
     * @param arrayAttr array attribute containing multiple Attribute instances.
     * @param name name of the Attributes collection
     * @param jsonDocument json document in which we emplace the new JsonArray.
     */
    private static void addJsonObject(Attribute<?> arrayAttr,String name, JsonArray jsonDocument){
        if (arrayAttr instanceof TypeAttribute){
            if (((TypeAttribute)arrayAttr).getType() instanceof NumberType){
                JsonObject obj = new JsonObject();
                obj.addProperty(name,(BigDecimal)((TypeAttribute)arrayAttr).getType().denormalize());
                jsonDocument.add(obj);
            }
            else{
                JsonObject obj = new JsonObject();
                obj.addProperty(name,arrayAttr.toString());
                jsonDocument.add(obj);
            }
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
        if (attr instanceof TypeAttribute){
            if (((TypeAttribute)attr).getType() instanceof NumberType){
                jsonDocument.addProperty(name,(BigDecimal)((TypeAttribute)attr).getType().denormalize());
            }
            else{
                jsonDocument.addProperty(name,attr.toString());
            }
        }
//        if (attr instanceof Attribute) {
//            Attribute attribute = (Attribute)attr;
//            jsonDocument.addProperty(name, attr.toString());
//
//        }
//        if (attr instanceof Content) {
//            Content content = (Content)attr;
//
//        }

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
                Map.Entry<String,JsonElement> ret = obj.entrySet().iterator().next();
                type = new NoOpType(ret.getValue().getAsString());
            }
            else {
                type = new NoOpType(element.getAsString());

            }
            attr = new TypeAttribute<>(type, key, true);
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
        doc.put(name,elementToAttribute(element), true, false);
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
        doc.put(name,attrs, true, false);
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