package datawave.query.attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.postprocessing.tf.TermOffsetMap;

/**
 * Test some document serialization and deserialization
 */
public class DocumentTest {

    @Test
    public void testDefaultReadWrite() throws IOException {
        Document d = buildDefaultDocument();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        d.write(dos);
        dos.flush();
        baos.flush();

        byte[] data = baos.toByteArray();

        dos.close();
        baos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        Document d2 = new Document();
        d2.readFields(dis);

        assertTrue(d2.containsKey("FOO"));
        assertNull(d2.getOffsetMap());
    }

    @Test
    public void testDefaultOffsetReadWrite() throws IOException {
        Document d = buildTermFrequencyDocument();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        d.write(dos);
        dos.flush();
        baos.flush();

        byte[] data = baos.toByteArray();

        dos.close();
        baos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        Document d2 = new Document();
        d2.readFields(dis);

        assertTrue(d2.containsKey("FOO"));
        assertNotNull(d2.getOffsetMap());
        assertNotNull(d2.getOffsetMap().getTermFrequencyList("big"));
        assertNotNull(d2.getOffsetMap().getTermFrequencyList("red"));
        assertNotNull(d2.getOffsetMap().getTermFrequencyList("dog"));
    }

    @Test
    public void testKryoReadWrite() {
        Document d = buildDefaultDocument();
        KryoDocumentSerializer ser = new KryoDocumentSerializer();
        byte[] data = ser.serialize(d);

        KryoDocumentDeserializer de = new KryoDocumentDeserializer();
        Document d2 = de.deserialize(new ByteArrayInputStream(data));
        assertTrue(d2.containsKey("FOO"));
        assertNull(d2.getOffsetMap());
    }

    @Test
    public void testKryoOffsetReadWrite() {
        Document d = buildTermFrequencyDocument();
        KryoDocumentSerializer ser = new KryoDocumentSerializer();
        byte[] data = ser.serialize(d);

        KryoDocumentDeserializer de = new KryoDocumentDeserializer();
        Document d2 = de.deserialize(new ByteArrayInputStream(data));
        assertTrue(d2.containsKey("FOO"));
        assertNotNull(d2.getOffsetMap());
        assertTrue(d2.getOffsetMap().getTermFrequencyKeySet().contains("big"));
        assertTrue(d2.getOffsetMap().getTermFrequencyKeySet().contains("red"));
        assertTrue(d2.getOffsetMap().getTermFrequencyKeySet().contains("dog"));
    }

    @Test
    public void testMergeDocumentsWithTermOffsetMaps() {
        Document d1 = buildDefaultDocument();
        Document d2 = buildDefaultDocument();

        TermOffsetMap firstOffsetMap = getTermOffsetMap();

        TermOffsetMap secondOffsetMap = new TermOffsetMap();
        secondOffsetMap.putTermFrequencyList("see", buildTfList("TEXT", 4));
        secondOffsetMap.putTermFrequencyList("spot", buildTfList("TEXT", 5));
        secondOffsetMap.putTermFrequencyList("run", buildTfList("TEXT", 6));

        d1.setOffsetMap(firstOffsetMap);
        d2.setOffsetMap(secondOffsetMap);

        d1.putAll(d2, false);

        TermOffsetMap merged = d1.getOffsetMap();
        assertEquals(6, merged.getTermFrequencyKeySet().size());
        assertEquals(Sets.newHashSet("see", "spot", "run", "big", "red", "dog"), merged.getTermFrequencyKeySet());
    }

    protected Document buildDefaultDocument() {
        Document d = new Document();
        d.put("FOO", new Content("value", new Key("datatype\0uid"), true));
        return d;
    }

    protected Document buildTermFrequencyDocument() {
        Document d = buildDefaultDocument();
        // add term offset map
        d.setOffsetMap(getTermOffsetMap());
        return d;
    }

    protected TermOffsetMap getTermOffsetMap() {
        TermOffsetMap offsets = new TermOffsetMap();
        offsets.putTermFrequencyList("big", buildTfList("TEXT", 1));
        offsets.putTermFrequencyList("red", buildTfList("TEXT", 2));
        offsets.putTermFrequencyList("dog", buildTfList("TEXT", 3));
        return offsets;
    }

    protected TermFrequencyList buildTfList(String field, int... offsets) {
        TermFrequencyList.Zone zone = buildZone(field);
        List<TermWeightPosition> position = buildTermWeightPositions(offsets);
        return new TermFrequencyList(Maps.immutableEntry(zone, position));
    }

    protected TermFrequencyList.Zone buildZone(String field) {
        return new TermFrequencyList.Zone(field, true, "shard\0datatype\0uid");
    }

    protected List<TermWeightPosition> buildTermWeightPositions(int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(true).build());
        }
        return list;
    }

}
