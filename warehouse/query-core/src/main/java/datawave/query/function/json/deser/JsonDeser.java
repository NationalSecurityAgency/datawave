package datawave.query.function.json.deser;


import com.google.common.collect.Maps;
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
import datawave.query.attributes.DocumentKey;
import datawave.query.attributes.TimingMetadata;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class JsonDeser implements com.google.gson.JsonSerializer<Document>,com.google.gson.JsonDeserializer<Document>{
    private static final Logger log = Logger.getLogger(JsonDeser.class);

    protected static final ConstuctorCacheMiss constructorMissFx = new ConstuctorCacheMiss();
    protected static final AttributeConstructorCacheMiss attributeMissFx = new AttributeConstructorCacheMiss();
    public static final String TYPE_METADATA = "type.metadata";
    public static final String TYPE_TYPE = "type.type";
    public static final String TYPE_DATA = "type.data";
    public static final String DOC_KEY = "doc.key";
    public static final String DATAWAVE_QUERY_ATTRIBUTES_DOCUMENT_KEY = "datawave.query.attributes.DocumentKey";
    public static final String DATAWAVE_QUERY_ATTRIBUTES_TIMING_METADATA = "datawave.query.attributes.TimingMetadata";
    protected static final ConcurrentHashMap<String, Constructor> constructorCache = new ConcurrentHashMap<>();

    private static class Holder {
        private static final JsonDeser INSTANCE = new JsonDeser();
    }

    public static JsonDeser getInstance(){
        return Holder.INSTANCE;
    }

    protected JsonDeser(){

    }

    /**
     * Add the raw attribute data
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    protected void addAttributeData(Attribute<?> attr,String name, JsonObject jsonObject) {
        if (attr instanceof TypeAttribute){
            datawave.data.type.Type t = ((TypeAttribute)attr).getType();
            if (t.getClass() != NoOpType.class)
                jsonObject.addProperty(TYPE_METADATA,t.getClass().getCanonicalName());
        }else{
            jsonObject.addProperty(TYPE_TYPE,attr.getClass().getCanonicalName());
        }


        jsonObject.addProperty(TYPE_DATA,attr.getData().toString());

    }

    /**
     * Add the metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    void addAttributeMetadata(Attribute<?> attr, Key docKey, String name, JsonObject jsonObject){
        if ( attr.isMetadataSet() ){
            Key metadata = attr.getMetadata();
            if (null != docKey && metadata.equals(docKey)){
                jsonObject.add(DOC_KEY, new JsonObject());
            }
            else {
                JsonObject key = new JsonObject();
                /**
                 * These parts of the key will not be included to save space
                 key.addProperty("row", metadata.getRow().toString());
                 key.addProperty("cf", metadata.getColumnFamily().toString());
                 key.addProperty("cq", metadata.getColumnQualifier().toString());
                 */
                key.addProperty("cv", metadata.getColumnVisibility().toString());
                key.addProperty("timestamp", metadata.getTimestamp());
                jsonObject.add(DOC_KEY, key);
            }

        }
    }

    /**
     * Add the data and metadata to the attribute
     * @param attr data to add new property to
     * @param name name of property to add
     * @param jsonObject json object
     */
    void addAttribute(Attribute<?> attr, Key docKey, String name, JsonObject jsonObject) {
        addAttributeData(attr,name,jsonObject);
        addAttributeMetadata(attr,docKey,name,jsonObject);
    }


    /**
     * Typed conversion of the JSON array.
     * @param arrayAttr array attribute containing multiple Attribute instances.
     * @param name name of the Attributes collection
     * @param jsonDocument json document in which we emplace the new JsonArray.
     */
    void addJsonObject(Attribute<?> arrayAttr,Key docKey,String name, JsonArray jsonDocument){
        if (arrayAttr instanceof TypeAttribute && ((TypeAttribute)arrayAttr).getType() instanceof NumberType){

            JsonObject obj = new JsonObject();
            obj.addProperty(TYPE_DATA,(BigDecimal)((TypeAttribute)arrayAttr).getType().denormalize());
            addAttributeMetadata(arrayAttr,docKey, name,obj);
            jsonDocument.add(obj);


        }
        else
        {
            JsonObject obj = new JsonObject();
            addAttribute(arrayAttr,docKey,name,obj);
            jsonDocument.add(obj);
        }

    }

    /**
     * Adds a new JsonElement to the jsonDocument.
     * @param attr attribute to convert
     * @param name name of the attribute
     * @param jsonDocument json document.
     */
    private void addJsonObject(Attribute<?> attr,Key docKey, String name, JsonObject jsonDocument){
        if (attr instanceof Attributes){
            // we have an array
            JsonArray array = new JsonArray();
            for(Attribute<?> subA : ((Attributes)attr).getAttributes()){
                addJsonObject(subA,docKey, name,array);
            }
            jsonDocument.add(name,array);
        }
        else if (attr instanceof TypeAttribute && ((TypeAttribute)attr).getType() instanceof NumberType){

            JsonObject newObj = new JsonObject();
            newObj.addProperty(TYPE_DATA,(BigDecimal)((TypeAttribute)attr).getType().denormalize());
            addAttributeMetadata(attr,docKey, name,newObj);
            jsonDocument.add(name,newObj);

        }
        else{
            JsonObject newObj = new JsonObject();
            addAttribute(attr,docKey, name,newObj);
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
        Key docKey = null;
        if (document.isMetadataSet()){
            JsonObject newObj = new JsonObject();
            docKey = document.getMetadata();
            addAttributeMetadata(document,null,"key",newObj);
            jsonDocument.add(DOC_KEY,newObj);
        }
        for(Map.Entry<String, Attribute<?>> entry : document.getDictionary().entrySet()){
            Attribute<?> attr = entry.getValue();
            addJsonObject(attr,docKey,entry.getKey(),jsonDocument);
        }



        return jsonDocument;
    }

    private Attribute<?> constructAttribute(String attributeTypeString, JsonElement data, Key key){
        if (DATAWAVE_QUERY_ATTRIBUTES_DOCUMENT_KEY.equals(attributeTypeString)) {
            DocumentKey docKey = new DocumentKey(key,true);
            return docKey;
        }
        else if (DATAWAVE_QUERY_ATTRIBUTES_TIMING_METADATA.equals(attributeTypeString)) {
            return new TimingMetadata();
        }
        else{
            Constructor constructor = null;
            try {
                constructor = constructorCache.computeIfAbsent(attributeTypeString,attributeMissFx);
                return (Attribute<?>)  constructor.newInstance(data.getAsString(),key,true);
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     */
    Attribute<?> elementToAttribute(JsonElement element, Key docKey) {
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
                    //key = new Key(jsonKey.get("row").getAsString(),jsonKey.get("cf").getAsString(),jsonKey.get("cq").getAsString(),jsonKey.get("cv").getAsString(),jsonKey.get("timestamp").getAsLong());
                    // we don't need the full key, so we let the row, cf, and cq be empty
                    key = new Key("","","",jsonKey.get("cv").getAsString(),jsonKey.get("timestamp").getAsLong());
                }else if (obj.has(DOC_KEY)){
                    key = docKey;
                }
                if (obj.has(TYPE_METADATA)){
                    typeString = obj.get(TYPE_METADATA).getAsString();
                }
                if (obj.has(TYPE_TYPE)){
                    attributeTypeString = obj.get(TYPE_TYPE).getAsString();
                }
                JsonElement data = obj.get(TYPE_DATA);
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

                        attr = constructAttribute(attributeTypeString,data,key);
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
     * @param docKey
     */
    void populateAttribute(JsonElement element, String name, Document doc, Key docKey){
        doc.put(Maps.immutableEntry(name,elementToAttribute(element,docKey)),true);
    }

    /**
     * Populate an array of Attributes
     * @param array JsonArray
     * @param name name of the Attributes
     * @param doc document to emplace the attributes.
     * @param docKey
     */
    void populateAttributes(JsonArray array, String name, Document doc, Key docKey){
        final Attributes attrs = new Attributes(true);
        array.iterator().forEachRemaining( x -> {
            attrs.add(elementToAttribute(x,docKey));
        });
        doc.put(Maps.immutableEntry(name,attrs),true);
    }
    @Override
    public Document deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        Key key = null;
        if (jsonElement.isJsonObject()){
            JsonObject obj = (JsonObject)jsonElement;
            if (obj.has(DOC_KEY)){
                JsonObject jsonKey = obj.getAsJsonObject(DOC_KEY).getAsJsonObject(DOC_KEY);
                // we don't need the full key
                key = new Key("","","",jsonKey.get("cv").getAsString(),jsonKey.get("timestamp").getAsLong());
            }
            obj.remove(DOC_KEY);
        }
        final Document doc = new Document(key,true);
        if (jsonElement instanceof JsonObject){
            final Key docKey = key;
            ((JsonObject)jsonElement).entrySet().stream().forEach(
                    x->{ // Entry<String,JsonElement>
                        if (x.getValue().isJsonArray()){
                            // we have Attributes
                            populateAttributes((JsonArray)x.getValue(),x.getKey(),doc, docKey);
                        }
                        else{
                            populateAttribute(x.getValue(),x.getKey(),doc, docKey);
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