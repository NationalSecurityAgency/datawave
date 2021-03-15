package datawave.query.jexl.functions;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TermFrequencyListTest {
    
    @Test
    public void testGetEventId() {
        // base case
        Key tfKey = new Key("shard", "tf", "datatype\0uid\0value\0FIELD");
        String expected = "shard\0datatype\0uid";
        assertEquals(expected, TermFrequencyList.getEventId(tfKey));
        
        // child doc case
        tfKey = new Key("shard", "tf", "datatype\0uid0.1\0value\0FIELD");
        expected = "shard\0datatype\0uid0.1";
        assertEquals(expected, TermFrequencyList.getEventId(tfKey));
    }
}
