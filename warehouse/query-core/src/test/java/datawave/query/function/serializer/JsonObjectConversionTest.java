package datawave.query.function.serializer;

import com.google.common.collect.Maps;
import datawave.data.type.NoOpType;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.serializer.JsonObjectSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.tables.document.batch.DocumentKeyConversion;
import datawave.query.tables.serialization.JsonDocument;
import datawave.query.tables.serialization.RawJsonDocument;
import datawave.query.tables.serialization.SerializedDocument;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class JsonObjectConversionTest {


    Document testDocument = null;

    Map.Entry<Key, Value> kv = null;
    private Key testKey;

    @Before
    public void setupKey() throws IOException {
        testKey = new Key("a","b","c");
        testDocument = new Document(testKey,true);
        testDocument.put("attribute",new TypeAttribute(new NoOpType("type"), testKey,true));
        JsonObjectSerializer serializer = new JsonObjectSerializer(false);
        kv = Maps.immutableEntry(testKey,new Value(DocumentSerialization.writeBodyWithHeader(serializer.serialize(testDocument),0,true)));
    }

    @Test(expected=NullPointerException.class)
    public void testNullInputTk(){

        TKeyValue kv = null;
        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);
    }

    @Test(expected=NullPointerException.class)
    public void testNullInputKV(){
        Map.Entry<Key, Value> kv = Map.entry(null,null);
        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);
    }
    @Test(expected=NullPointerException.class)
    public void testNullInputKVKey(){
        Map.Entry<Key, Value> kv = Map.entry(new Key(),null);
        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);
    }
    @Test(expected=NullPointerException.class)
    public void testNullInputKVValue(){
        Map.Entry<Key, Value> kv = Map.entry(null,new Value());
        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);
    }
    @Test(expected=NullPointerException.class)
    public void testNullInputKVEntry(){
        Map.Entry<Key, Value> kv = null;

        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);
    }

    @Test
    public void testConversion(){
        SerializedDocumentIfc doc = DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,kv);

        Assert.assertTrue(doc instanceof RawJsonDocument);

        Document comparison = ((RawJsonDocument)doc).getAsDocument();

        // this does not work since document conversion is lossy
//        Assert.assertEquals(testDocument, comparison);
        Assert.assertEquals(testDocument.get("attribute").getData(), comparison.get("attribute").getData());

    }

    @Test
    public void testEmptyDocument() throws IOException {
        testDocument = new Document(testKey,true);
        JsonDocumentSerializer serializer = new JsonDocumentSerializer(false);
        kv = Maps.immutableEntry(testKey,new Value(DocumentSerialization.writeBodyWithHeader(serializer.serialize(testDocument),0,true)));
        SerializedDocumentIfc doc = DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,false,kv);

        Assert.assertTrue(doc instanceof JsonDocument);

        Document comparison = ((JsonDocument)doc).getAsDocument();

        Assert.assertEquals(testDocument, comparison);
        Assert.assertEquals(testDocument.size(), comparison.size());
        Assert.assertEquals(0,testDocument.size());

    }


    @Test(expected=NullPointerException.class)
    public void testNullArguments() throws IOException {
        TKeyValue tkv = null;
        DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,false,tkv);


    }


    @Test
    public void testComputeIdentifier(){
        Key key = new Key("20160605_0","csv\u0000-101v35.-or0ktr.-au6c06","FIELD\u0000VALUE");
        JsonObjectSerializer serializer = new JsonObjectSerializer(false);
        byte [] docdata = serializer.serialize(testDocument);
        var val = serializer.getValue(key,docdata);
        RawJsonDocument rawdoc = (RawJsonDocument) DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,true,Maps.immutableEntry(key,val));
        Assert.assertEquals("0605_0-101v35.-or0ktr.-au6c06",new String(rawdoc.getIdentifier()).trim());

    }

}
