package datawave.query.jexl.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import com.google.common.collect.Maps;

import datawave.ingest.protobuf.TermWeightPosition;

public class TermFrequencyListTest {

    @Test
    public void testDeser() throws IOException, ClassNotFoundException {
        String eventId = "shard\0type\0uid";
        List<TermWeightPosition> positions = new ArrayList<>();
        positions.add(new TermWeightPosition.Builder().setOffset(15).setZeroOffsetMatch(true).build());
        TermFrequencyList.Zone zone = new TermFrequencyList.Zone("BODY", true, eventId);
        TermFrequencyList tfl = new TermFrequencyList(Maps.immutableEntry(zone, positions));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(tfl);
        oos.flush();

        byte[] data = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        TermFrequencyList next = (TermFrequencyList) ois.readObject();

        assertTrue(next.zones().contains(zone));
    }

    @Test
    public void testGetEventId() {
        // base case
        Key tfKey = new Key("shard", "tf", "datatype\0uid123\0value\0FIELD");
        String expected = "shard\0datatype\0uid123";
        test(expected, tfKey);

        // child doc case
        tfKey = new Key("shard", "tf", "datatype\0uid123.1\0value\0FIELD");
        expected = "shard\0datatype\0uid123.1";
        test(expected, tfKey);
    }

    private void test(String expected, Key key) {
        String eventId = TermFrequencyList.getEventId(key);
        assertEquals(expected, eventId);
    }
}
