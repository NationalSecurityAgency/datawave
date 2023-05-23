package datawave.query.function.json.deser;

import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeBag;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentJsonDeserializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

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
    public void testMultiField(){
        // validate that json arrays are converted into attributes.
        final String json = "{ \"fieldA\" : [ 25, 26 ] , \"fieldB\" : \"stringvalue\"}";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Attribute<?> attr = doc.get("fieldA");
        Assert.assertNotNull(attr);
        Assert.assertEquals(attr.getClass(), Attributes.class);
        Set<NumberType> requiredValues = new HashSet<>();
        requiredValues.add(new NumberType("25"));
        requiredValues.add(new NumberType("26"));
        ((Attributes)attr).getAttributes().forEach( attrInSet -> { Assert.assertTrue(attrInSet +" does not exist", requiredValues.remove(attrInSet.getData())); });
        Assert.assertEquals(0,requiredValues.size() );
    }

    @Test
    public void testMultiFieldMultiType(){
        // validate that json arrays are converted into attributes.
        final String json = "{ \"fieldA\" : [ 25, 26, \"a\"] , \"fieldB\" : \"stringvalue\"}";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Attribute<?> attr = doc.get("fieldA");
        Assert.assertNotNull(attr);
        Assert.assertEquals(attr.getClass(), Attributes.class);
        Set<Type<?>> requiredValues = new HashSet<>();
        requiredValues.add(new NumberType("25"));
        requiredValues.add(new NumberType("26"));
        requiredValues.add(new NoOpType("a"));
        ((Attributes)attr).getAttributes().forEach( attrInSet -> { Assert.assertTrue(attrInSet +" does not exist", requiredValues.remove(attrInSet.getData())); });
        Assert.assertEquals(0,requiredValues.size() );
    }

    @Test
    public void testEmptyDocument(){
        final String json = "{ }";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(0, doc.size());
    }

    @Test
    public void testNullDocument(){
        final String json = "";
        DocumentJsonDeserializer deser = new DocumentJsonDeserializer();
        Document doc = deser.deserialize(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        Assert.assertNull(doc);
    }
}
