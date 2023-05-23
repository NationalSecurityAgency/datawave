package datawave.query.tables.serialization;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.dataImpl.thrift.TKey;
import org.junit.Assert;
import org.junit.Test;

public class RawJsonDocumenTest {

    static final byte [] TEST_BYTE = new byte[]{ (byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};
    @Test
    public void testConstruction(){
        var doc = new RawJsonDocument("docA", new TKey(), TEST_BYTE,7);

        Assert.assertEquals("docA",doc.get());
        Assert.assertArrayEquals(TEST_BYTE,doc.getIdentifier());
        Assert.assertEquals(7L,doc.size());
    }

    @Test(expected = NullPointerException.class)
    public void testNullKey(){
        var doc = new RawJsonDocument("docA", null, TEST_BYTE,7);

        //Assert.assertNotNull(doc.computeKey());
    }

    @Test
    public void TestDocumentConversion(){

        String docString = "{\"json_field\": \"json_value\"}";

        var doc = new RawJsonDocument(docString, new TKey(), TEST_BYTE,7);


        Document pojoDoc = doc.getAsDocument();

        Assert.assertNotNull(pojoDoc);
        var attribute = pojoDoc.get("json_field");
        Assert.assertNotNull(attribute);

        Assert.assertEquals("json_value",attribute.getData().toString());

    }

    @Test
    public void TestInvalidDocumentConversion(){

        String docString = "{json_field\": \"json_value\"}";

        var doc = new RawJsonDocument(docString, new TKey(), TEST_BYTE,7);


        Document pojoDoc = doc.getAsDocument();

        Assert.assertNotNull(pojoDoc);
        var attribute = pojoDoc.get("json_field");
        Assert.assertNull(attribute);

    }
}
