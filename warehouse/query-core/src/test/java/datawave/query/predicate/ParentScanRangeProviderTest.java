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
    
    private final Key childDocKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12");
    private final Key childDocKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12", "FIELD");
    private final Key childDocKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12", "FIELD\0value");
    
    private final Key grandchildDocKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34");
    private final Key grandchildDocKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34", "FIELD");
    private final Key grandchildDocKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34", "FIELD\0value");
    
    private final ScanRangeProvider rangeProvider = new ParentScanRangeProvider();
    
    @Test
    public void testGetStartKey() {
        assertEquals(docKey, rangeProvider.getStartKey(docKey));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyFieldValue));
        
        // docKey is the parent of the childDocKey
        assertEquals(docKey, rangeProvider.getStartKey(childDocKey));
        assertEquals(docKey, rangeProvider.getStartKey(childDocKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(childDocKeyFieldValue));
        
        // childDocKey is the parent of the grandchildDockey
        assertEquals(childDocKey, rangeProvider.getStartKey(grandchildDocKey));
        assertEquals(childDocKey, rangeProvider.getStartKey(grandchildDocKeyField));
        assertEquals(childDocKey, rangeProvider.getStartKey(grandchildDocKeyFieldValue));
    }
    
    @Test
    public void testGetStopKey() {
        Key expectedStopKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok\0");
        
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKey));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKeyField));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(docKeyFieldValue));
        
        expectedStopKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12\0");
        
        assertEquals(expectedStopKey, rangeProvider.getStopKey(childDocKey));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(childDocKeyField));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(childDocKeyFieldValue));
        
        expectedStopKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0");
        
        assertEquals(expectedStopKey, rangeProvider.getStopKey(grandchildDocKey));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(grandchildDocKeyField));
        assertEquals(expectedStopKey, rangeProvider.getStopKey(grandchildDocKeyFieldValue));
    }
    
    @Test
    public void testGetScanRange() {
        Key start = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok");
        Key end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok\0");
        Range expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getScanRange(docKey));
        assertEquals(expectedRange, rangeProvider.getScanRange(docKeyField));
        assertEquals(expectedRange, rangeProvider.getScanRange(docKeyFieldValue));
        
        end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12\0");
        expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getScanRange(childDocKey));
        assertEquals(expectedRange, rangeProvider.getScanRange(childDocKeyField));
        assertEquals(expectedRange, rangeProvider.getScanRange(childDocKeyFieldValue));
        
        start = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12");
        end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0");
        expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getScanRange(grandchildDocKey));
        assertEquals(expectedRange, rangeProvider.getScanRange(grandchildDocKeyField));
        assertEquals(expectedRange, rangeProvider.getScanRange(grandchildDocKeyFieldValue));
    }
}
