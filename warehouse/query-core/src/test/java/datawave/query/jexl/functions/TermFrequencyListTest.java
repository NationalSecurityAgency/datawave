package datawave.query.jexl.functions;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

public class TermFrequencyListTest {

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
