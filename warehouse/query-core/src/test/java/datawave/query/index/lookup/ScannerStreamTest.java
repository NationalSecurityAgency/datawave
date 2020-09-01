package datawave.query.index.lookup;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ScannerStreamTest {
    
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    
    @Test
    public void testHasNextPeekIteration() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090704");
        ScannerStream stream = buildScannerStream(elements.iterator());
        
        while (stream.hasNext()) {
            stream.peek();
            stream.next();
        }
        assertFalse(stream.hasNext());
    }
    
    // Repeated peek() calls followed by a next() should return the same element.
    @Test
    public void testPeekingIteratorContract() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090606");
        ScannerStream stream = buildScannerStream(elements.iterator());
        
        Tuple2<String,IndexInfo> top = stream.peek();
        for (int ii = 0; ii < 25; ii++) {
            assertEquals(top, stream.peek());
        }
        assertEquals(top, stream.next());
    }
    
    // It's overkill to create a fresh stream before each seek, but standards.
    @Test
    public void testSeekByNext_dayRange() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090704");
        
        // Seek before the start
        ScannerStream stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090605"));
        
        // Seek to the start
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090606"));
        
        // Seek to the middle-ish
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090630", stream.seekByNext("20090630"));
        
        // Seek to the end
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090704", stream.seekByNext("20090704"));
        
        // Seek beyond the end
        stream = buildScannerStream(elements.iterator());
        assertNull(stream.seekByNext("20090705"));
    }
    
    // Seek by a day range returned from the CondensedUidIterator. We don't support this
    // but it might be nice to have built in guards
    @Test
    public void testSeekByNext_altDayRange() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090704");
        
        // Seek before the start
        ScannerStream stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090605_"));
        
        // Seek to the start
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090606_"));
        
        // Seek to the middle-ish
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090630", stream.seekByNext("20090630_"));
        
        // Seek to the end
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090704", stream.seekByNext("20090704_"));
        
        // Seek beyond the end
        stream = buildScannerStream(elements.iterator());
        assertNull(stream.seekByNext("20090705_"));
    }
    
    @Test
    public void testSeekByNext_shardRange() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090704");
        
        // Seek before the start
        ScannerStream stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090605_1"));
        
        // Seek to the start
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090606_1"));
        
        // Seek to the middle-ish
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090630", stream.seekByNext("20090630_1"));
        
        // Seek to the end
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090704", stream.seekByNext("20090704_1"));
        
        // Seek beyond the end
        stream = buildScannerStream(elements.iterator());
        assertNull(stream.seekByNext("20090705_1"));
    }
    
    @Test
    public void testSeekByNext_altShardRange() {
        List<Tuple2<String,IndexInfo>> elements = createIter("20090606", "20090704");
        
        // Seek before the start
        ScannerStream stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090605_11"));
        
        // Seek to the start
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090606", stream.seekByNext("20090606_11"));
        
        // Seek to the middle-ish
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090630", stream.seekByNext("20090630_11"));
        
        // Seek to the end
        stream = buildScannerStream(elements.iterator());
        assertEquals("20090704", stream.seekByNext("20090704_11"));
        
        // Seek beyond the end
        stream = buildScannerStream(elements.iterator());
        assertNull(stream.seekByNext("20090705_11"));
    }
    
    public List<Tuple2<String,IndexInfo>> createIter(String startDate, String endDate) {
        try {
            ShardQueryConfiguration config = new ShardQueryConfiguration();
            config.setBeginDate(sdf.parse(startDate));
            config.setEndDate(sdf.parse(endDate));
            
            JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
            
            return RangeStream.createFullFieldIndexScanList(config, node);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public ScannerStream buildScannerStream(Iterator<Tuple2<String,IndexInfo>> iter) {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        return ScannerStream.withData(iter, node);
    }
}
