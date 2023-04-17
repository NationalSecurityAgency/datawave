package datawave.query.attributes;

import datawave.data.type.IpAddressType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.util.IpAddress;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;

public class DocumentKryoSerializationTest {
    private final static byte[] EMPTY_BYTES = new byte[] {};
    
    @Test
    public void testSerialializeAndDeserialize() {
        Key key = new Key(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, new ColumnVisibility("SOMEVAL|(OTHER&CAR&BIRD&(A|B|C))"), System.currentTimeMillis());
        Random random = new Random(42);
        RandomStringGenerator rg = new RandomStringGenerator.Builder().usingRandom(random::nextInt).withinRange('a', 'z').build();
        String field1 = "field-1";
        String field2 = "field-2";
        IpAddressType ip1 = new IpAddressType();
        ip1.setDelegateFromString(ip1.normalize("100.2.10.*"));
        TypeAttribute<String> type1 = new TypeAttribute<>(new LcNoDiacriticsType("value-1"), key, true);
        TypeAttribute<String> type2 = new TypeAttribute<>(new LcNoDiacriticsType("value-2"), key, true);
        TypeAttribute<IpAddress> type3 = new TypeAttribute<>(ip1, key, true);
        Document doc = new Document();
        TimingMetadata timing = new TimingMetadata();
        timing.setHost("localhost");
        timing.setMetadata(key);
        timing.setNextCount(100);
        timing.setSeekCount(1000);
        timing.setSourceCount(100);
        timing.setYieldCount(100);
        timing.addStageTimer("STAGE_1", new Numeric("3", key, true));
        timing.addStageTimer("STAGE_2", new Numeric("100", key, true));
        timing.addStageTimer("STAGE_3", new Numeric("300", key, true));
        doc.put(field1, type1);
        // doc.put(field2, type2);
        // doc.put("TIMING_METADATA", timing);
        
        KryoDocumentSerializer serializer = new KryoDocumentSerializer();
        KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
        byte[] bytes = serializer.serialize(doc);
        Document testDoc = deserializer.deserialize(new ByteArrayInputStream(bytes));
        
        Assert.assertEquals("field/value 1", testDoc.get("field-1"), type1);
        Assert.assertEquals("field/value 2", testDoc.get("field-2"), type1);
    }
}
