package datawave.query.function.json.deser;

import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentJsonDeserializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class JsonDocumentSerializerTest {

    @Test
    public void testConversion(){
        final String json = "{ \"fieldA\" : 25 , \"fieldB\" : \"stringvalue\"}";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Attribute<?> attr = doc.get("fieldA");
        Assert.assertNotNull(attr);
        Assert.assertEquals(new NumberType("25"),attr.getData());
    }

    @Test
    public void testString(){
        final String json = "{ \"fieldA\" : 25 , \"fieldB\" : \"stringvalue\"}";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Attribute<?> attr = doc.get("fieldB");
        Assert.assertNotNull(attr);
        Assert.assertEquals(new NoOpType("stringvalue"),attr.getData());
    }

    @Test
    public void testEmptyDocument(){
        final String json = "{ }";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(0, doc.size());
    }
}
