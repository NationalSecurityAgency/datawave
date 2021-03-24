package datawave.query.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link QueryIterator}.
 *
 * Currently only covers some helper methods.
 */
public class QueryIteratorTest {
    
    @Test
    public void testIsDocumentSpecificRange_withInfiniteKeys() {
        // Test the case of an infinite start key
        Key end = new Key("20190314_0", "dataType\0doc0\0");
        Range infiniteStartRange = new Range(null, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(infiniteStartRange));
        
        // Test the case of an infinite end key
        Key start = new Key("20190314_0", "dataType\0doc0");
        Range infiniteEndRange = new Range(start, null);
        assertFalse(QueryIterator.isDocumentSpecificRange(infiniteEndRange));
    }
    
    @Test
    public void testIsDocumentSpecificRange_spansMultipleRows() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_9", "dataType\0doc0\0");
        Range multipleRowRange = new Range(start, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(multipleRowRange));
    }
    
    @Test
    public void testIsDocumentSpecificRange_withDocRange() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_0", "dataType\0doc0\0");
        Range docRange = new Range(start, end);
        assertTrue(QueryIterator.isDocumentSpecificRange(docRange));
    }
    
    @Test
    public void testIsDocumentSpecificRange_withShardRange() {
        Key start = new Key("20190314_0");
        Key end = new Key("20190314_0");
        Range shardRange = new Range(start, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(shardRange));
    }
    
    @Test
    public void testIsDocumentSpecificRange_withRebuiltShardRange() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_0\u0000");
        Range range = new Range(start, false, end, false);
        assertFalse(QueryIterator.isDocumentSpecificRange(range));
    }
    
    /**
     * <pre>
     * Shard key format
     * Key.row = shard
     * Key.columnFamily = datatype\0docId
     * Key.columnQualifier = field\0value
     * </pre>
     */
    @Test
    public void testRowColfamToString() {
        String expected = "20190314_0 test%00;doc0:FOO%00;bar";
        
        Text row = new Text("20190314_0");
        Text cf = new Text("test\0doc0");
        Text cq = new Text("FOO\0bar");
        Key key = new Key(row, cf, cq);
        
        String parsed = QueryIterator.rowColFamToString(key);
        assertEquals(expected, parsed);
        
        // Test the null case as well
        assertEquals("null", QueryIterator.rowColFamToString(null));
    }
}
