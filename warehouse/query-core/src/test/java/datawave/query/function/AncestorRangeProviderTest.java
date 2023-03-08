package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AncestorRangeProviderTest {
    
    private final Key docKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok");
    private final Key docKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD");
    private final Key docKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok", "FIELD\0value");
    
    private final Key childDocKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12");
    private final Key childDocKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12", "FIELD");
    private final Key childDocKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12", "FIELD\0value");
    
    private final Key grandchildDocKey = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34");
    private final Key grandchildDocKeyField = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34", "FIELD");
    private final Key grandchildDocKeyFieldValue = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34", "FIELD\0value");
    
    private final RangeProvider rangeProvider = new AncestorRangeProvider();
    
    @Test
    public void testGetStartKey() {
        assertEquals(docKey, rangeProvider.getStartKey(docKey));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(docKeyFieldValue));
        
        assertEquals(docKey, rangeProvider.getStartKey(childDocKey));
        assertEquals(docKey, rangeProvider.getStartKey(childDocKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(childDocKeyFieldValue));
        
        assertEquals(docKey, rangeProvider.getStartKey(grandchildDocKey));
        assertEquals(docKey, rangeProvider.getStartKey(grandchildDocKeyField));
        assertEquals(docKey, rangeProvider.getStartKey(grandchildDocKeyFieldValue));
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
        
        assertEquals(expectedRange, rangeProvider.getRange(docKey));
        assertEquals(expectedRange, rangeProvider.getRange(docKeyField));
        assertEquals(expectedRange, rangeProvider.getRange(docKeyFieldValue));
        
        end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12\0");
        expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getRange(childDocKey));
        assertEquals(expectedRange, rangeProvider.getRange(childDocKeyField));
        assertEquals(expectedRange, rangeProvider.getRange(childDocKeyFieldValue));
        
        end = new Key("row", "datatype\0d8zay2.-3pnndm.-anolok.12.34\0");
        expectedRange = new Range(start, true, end, false);
        
        assertEquals(expectedRange, rangeProvider.getRange(grandchildDocKey));
        assertEquals(expectedRange, rangeProvider.getRange(grandchildDocKeyField));
        assertEquals(expectedRange, rangeProvider.getRange(grandchildDocKeyFieldValue));
    }
}
