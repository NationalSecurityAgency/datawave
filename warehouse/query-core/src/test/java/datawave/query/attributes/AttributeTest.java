package datawave.query.attributes;

import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AttributeTest {

    protected void testToKeep(Attribute<?> attr, boolean expected) {
        testDefaultSerialization(attr, expected);
        testKryoSerialization(attr, expected);
    }

    private void testKryoSerialization(Attribute<?> attr, boolean expected) {
        KryoDocumentSerializer ser = new KryoDocumentSerializer();
        KryoDocumentDeserializer de = new KryoDocumentDeserializer();

        Document d = new Document();
        d.put("KEY", attr);

        byte[] data = ser.serialize(d);
        Document next = de.deserialize(new ByteArrayInputStream(data));

        Attribute<?> nextAttr = next.get("KEY");
        assertEquals(expected, nextAttr.isToKeep());
    }

    private void testDefaultSerialization(Attribute<?> attr, boolean expected) {
        try {
            Document d = new Document();
            d.put("KEY", attr);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            d.write(out);
            baos.flush();
            byte[] data = baos.toByteArray();

            Document next = new Document();
            next.readFields(new DataInputStream(new ByteArrayInputStream(data)));

            Attribute<?> nextAttr = next.get("KEY");
            assertEquals(expected, nextAttr.isToKeep());

        } catch (Exception e) {
            fail("Test failed with exception: " + e.getMessage());
        }
    }
}
