package datawave.query.function.json.deser;


import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import datawave.data.type.BaseType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class JsonDeser implements com.google.gson.JsonSerializer<Document>,com.google.gson.JsonDeserializer<Document>{
    private static final Logger log = Logger.getLogger(JsonDeser.class);

    private static final ConstuctorCacheMiss constructorMissFx = new ConstuctorCacheMiss();
    private static final AttributeConstructorCacheMiss attributeMissFx = new AttributeConstructorCacheMiss();
    private static ConcurrentHashMap<String, Constructor> constructorCache = new ConcurrentHashMap<>();

    /**
     * Add the raw attribute data
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    private static void addAttributeData(Attribute<?> attr,String name, JsonObject jsonObject) {
        if (attr instanceof TypeAttribute){
            datawave.data.type.Type t = ((TypeAttribute)attr).getType();
            if (t.getClass() != NoOpType.class)
                jsonObject.addProperty("type.metadata",t.getClass().getCanonicalName());
        }else{
            jsonObject.addProperty("type.type",attr.getClass().getCanonicalName());
        }


        jsonObject.addProperty("type.data",attr.getData().toString());

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
            JsonObject key = new JsonObject();
//            key.addProperty("row",metadata.getRow().toString());
  //          key.addProperty("cf",metadata.getColumnFamily().toString());
    //        key.addProperty("cq",metadata.getColumnQualifier().toString());
            key.addProperty("cv",metadata.getColumnVisibility().toString());
            key.addProperty("timestamp",metadata.getTimestamp());
            jsonObject.add("key",key);

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
        else if (attr instanceof TypeAttribute && ((TypeAttribute)attr).getType() instanceof NumberType){

            jsonDocument.addProperty(name,(BigDecimal)((TypeAttribute)attr).getType().denormalize());
            addAttributeMetadata(attr,name,jsonDocument);
        }
        else{
            JsonObject newObj = new JsonObject();
            addAttribute(attr,name,newObj);
            jsonDocument.add(name,newObj);
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
        if (document.isMetadataSet()){
            addAttributeMetadata(document,"key",jsonDocument);
        }


        return jsonDocument;
    }

    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     */
    private static Attribute<?> elementToAttribute(JsonElement element) {
        Key key = new Key();
        Attribute<?> attr = null;
        if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()){
            NumberType type = new NumberType(element.getAsString());
            attr = new TypeAttribute<>(type,key,true);
        }
        else{
            BaseType<?> type = null;
            String typeString="", attributeTypeString="";
            if (element instanceof JsonObject){
                JsonObject obj = (JsonObject)element;
                if ( obj.has("key") ){ // it has metadata
                    JsonObject jsonKey = obj.getAsJsonObject("key");
                    key = new Key("","","",jsonKey.get("cv").getAsString(),jsonKey.get("timestamp").getAsLong());
                }
                if (obj.has("type.metadata")){
                    typeString = obj.get("type.metadata").getAsString();
                }
                if (obj.has("type.type")){
                    attributeTypeString = obj.get("type.type").getAsString();
                }
                JsonElement data = obj.get("type.data");
                if (data.isJsonPrimitive() && data.getAsJsonPrimitive().isNumber()){
                    NumberType primitiveType = new NumberType(data.getAsString());
                    attr = new TypeAttribute<>(primitiveType,key,true);
                }
                else {
                    if (typeString.isEmpty())
                        type = new NoOpType(data.getAsString());
                    else{
                        Constructor constructor = constructorCache.computeIfAbsent(typeString,constructorMissFx);;
                        try {

                            type = (BaseType<?>) constructor.newInstance(data.getAsString());
                        }catch(Exception e){
                            try {
                                type = (BaseType<?>) constructor.newInstance();
                                type.setDelegateFromString(data.getAsString());
                            }catch(Exception e1){
                                throw new RuntimeException(e1);
                            }
                        }
                    }
                    if (!attributeTypeString.isEmpty()){
                        Constructor constructor = null;
                        try {
                            constructor = constructorCache.computeIfAbsent(attributeTypeString,attributeMissFx);
                            attr = (Attribute<?>)  constructor.newInstance(data.getAsString(),key,true);
                        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }

                    }
                    else
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
        doc.put(name,elementToAttribute(element),true,true);
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
        doc.put(name,attrs,true,true);
    }
    @Override
    public Document deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        Key key = null;
        if (jsonElement.isJsonObject()){
            JsonObject obj = (JsonObject)jsonElement;
            if (obj.has("key")){
                JsonObject jsonKey = obj.getAsJsonObject("key");
                key = new Key(jsonKey.get("row").getAsString(),jsonKey.get("cf").getAsString(),jsonKey.get("cq").getAsString(),jsonKey.get("cv").getAsString(),jsonKey.get("timestamp").getAsLong());
            }
        }
        final Document doc = new Document(key,true);
        if (jsonElement instanceof JsonObject){
            ((JsonObject)jsonElement).entrySet().stream().forEach(
                    x->{ // Entry<String,JsonElement>
                        if (x.getValue().isJsonArray()){
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

    private static final class ConstuctorCacheMiss implements
            Function<String,Constructor>{

        @Override
        public Constructor apply(String typeString) {
            try {

                return Class.forName(typeString).asSubclass(BaseType.class).getConstructor(String.class);
            }catch(Exception e){
                try {
                    return Class.forName(typeString).asSubclass(BaseType.class).getConstructor();
                }catch(Exception e1){
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    private static final class AttributeConstructorCacheMiss implements
            Function<String,Constructor>{

        @Override
        public Constructor apply(String typeString) {
            try {
                return Class.forName(typeString).asSubclass(Attribute.class).getConstructor(String.class, Key.class, boolean.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

}