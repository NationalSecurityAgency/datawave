package datawave.query.function.json.deser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import datawave.data.type.BaseType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

public class DocumentJSONDeSerializer extends StdDeserializer<Document> {


    public static final LoadingCache<String, Constructor> constructorCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Constructor>() {
        @Override
        public Constructor load(String typeString) throws Exception {
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
    });

    public DocumentJSONDeSerializer(Class<Document> t) {
        super(t);
    }

    public DocumentJSONDeSerializer(){
        this(null);
    }


    /**
     * Populate an array of Attributes
     * @param array JsonArray
     * @param name name of the Attributes
     * @param doc document to emplace the attributes.
     */
    private static void populateAttributes(JsonNode array, String name, Document doc){
        final Attributes attrs = new Attributes(true);
        array.iterator().forEachRemaining( x -> {
            attrs.add(elementToAttribute(x));
        });
        doc.rawPut(name,attrs);
    }


    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     * @param name name of the typed attribute
     * @param doc document to emplace the JsonElement attribute.
     */
    private static void populateAttribute(JsonNode element, String name, Document doc){
        doc.rawPut(name,elementToAttribute(element));
    }

    @Override
    public Document deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode jp = jsonParser.getCodec().readTree(jsonParser);
        Document newDoc = new Document();
        jp.fields().forEachRemaining( entry ->{
                    final String fieldName = entry.getKey();
                    final JsonNode thisNode = entry.getValue();
                    if (thisNode.isArray()){
                        populateAttributes(thisNode,fieldName,newDoc);
                    }
                    else{
                        populateAttribute(thisNode,fieldName,newDoc);
                    }
                }

        );

        return newDoc;
    }

    /**
     * Converts the provided element to an attribute with the document
     * @param element json element we are populating into the Document
     */
    private static TypeAttribute<?> elementToAttribute(JsonNode element) {
        Key key = new Key();
        TypeAttribute<?> attr = null;
        if (element.isNumber() ){
            NumberType type = new NumberType(element.toString());
            attr = new TypeAttribute<>(type,key,true);
        }
        else{
            BaseType<?> type = null;
            String typeString = "";
                if ( element.has("key") ){ // it has metadata
                    JsonNode jsonKey = element.get("key");
                    key = new Key(jsonKey.get("row").toString(),jsonKey.get("cf").toString(),jsonKey.get("cq").toString(),jsonKey.get("cv").toString(),jsonKey.get("timestamp").longValue());
                }
                if (element.has("type.metadata")){
                    typeString = element.get("type.metadata").toString();
                }
                JsonNode data = element.get("type.data");
                if (data.isNumber()){
                    NumberType primitiveType = new NumberType(data.toString());
                    attr = new TypeAttribute<>(primitiveType,key,true);
                }
                else {
                    if (typeString.isEmpty())
                        type = new NoOpType(data.toString());
                    else{
                        Constructor constructor = null;
                        try {
                            constructor = constructorCache.get(typeString);
                            type = (BaseType<?>) constructor.newInstance(data.toString());
                        }catch(Exception e){
                            try {
                                constructor = constructorCache.get(typeString);
                                type = (BaseType<?>) constructor.newInstance();
                                type.setDelegateFromString(data.toString());
                            }catch(Exception e1){
                                throw new RuntimeException(e1);
                            }
                        }
                    }
                    attr = new TypeAttribute<>(type, key, true);
                }


        }
        return attr;
    }
}
