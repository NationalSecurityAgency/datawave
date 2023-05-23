package datawave.query.function.json.deser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentJsonDeserializer;
import datawave.query.function.serializer.JsonDocumentSerializer;
import datawave.query.tables.serialization.JsonDocument;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class JsonDocumentTest {

    static JsonParser parser = new JsonParser();
    byte [] identifier = new byte[0];
    @Test
    public void testConversion(){
        final String json = "{ \"fieldA\" : 25 , \"fieldB\" : \"stringvalue\"}";
        Key key = new Key("a","b");
        JsonObject obj = (JsonObject)parser.parse(json);
        JsonDocument jdoc = new JsonDocument(obj,key.toThrift(),identifier,2);
        Document doc = jdoc.getAsDocument();
        Attribute<?> attr = doc.get("fieldA");
        Assert.assertNotNull(attr);
        Assert.assertEquals(new NumberType("25"),attr.getData());

        Attribute<?> fieldB = doc.get("fieldB");
        Assert.assertNotNull(fieldB);
        Assert.assertEquals(new NoOpType("stringvalue"),fieldB.getData());

    }

    @Test(expected = ClassCastException.class)
    public void testUnknownType(){
        final String json = "{ \"fieldA\" : 25 , \"fieldB\" : \"stringvalue\"}";
        Key key = new Key("a","b");
        JsonObject obj = (JsonObject)parser.parse(json);
        JsonDocument jdoc = new JsonDocument(obj,key.toThrift(),identifier,2);
        String doc = jdoc.get();// class cast exception

    }

    @Test
    public void testEmptyDocument(){
        final String json = "{ }";
        Key key = new Key("a","b");
        JsonObject obj = (JsonObject)parser.parse(json);
        JsonDocument jdoc = new JsonDocument(obj,key.toThrift(),identifier,2);
        Document doc = jdoc.getAsDocument();
        Assert.assertEquals(0, doc.size());
    }

}
