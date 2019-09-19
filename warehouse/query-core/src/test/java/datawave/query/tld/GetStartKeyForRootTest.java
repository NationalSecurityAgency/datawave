package datawave.query.tld;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GetStartKeyForRootTest {
    
    @Test
    public void testApply() {
        String shard = "20190314_0";
        String doc = "datatype\u0000p1.p2.p3.c1.gc1";
        
        Key start = new Key(shard, doc);
        Key end = new Key(shard, doc + "\uffff");
        
        Range range = new Range(start, true, end, false);
        Key expected = new Key(shard, "datatype\u0000p1.p2.p3");
        
        assertEquals(expected, GetStartKeyForRoot.instance().apply(range));
    }
}
