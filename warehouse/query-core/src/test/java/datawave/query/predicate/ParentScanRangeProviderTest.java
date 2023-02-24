package datawave.query.predicate;

import datawave.query.function.ScanRangeProvider;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParentScanRangeProviderTest {
    
    private final Key docKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok");
    private final Key docKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD");
    private final Key docKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD\0value");
    
    private final Key childDocKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.23.34");
    
    private final ScanRangeProvider rangeProvider = new ParentScanRangeProvider();
    
    @Test
    public void testGetStartKey() {
        assertEquals(docKey, rangeProvider.getStartKey(docKey));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyFieldValue));
        
        // root uid is parsed from the child uid
        assertEquals(docKey, rangeProvider.getStartKey(childDocKey));
    }
    
    @Test
    public void testGetStopKey() {
        Key expectedStopKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok\0");
        
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKey));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKeyField));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKeyFieldValue));
    }
    
    @Test
    public void testGetScanRange() {
        Key start = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok");
        Key end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok\0");
        Range expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getScanRange(docKey));
        assertEquals(expectedRange, rangeProvider.getScanRange(docKeyField));
        assertEquals(expectedRange, rangeProvider.getScanRange(docKeyFieldValue));
    }
}
