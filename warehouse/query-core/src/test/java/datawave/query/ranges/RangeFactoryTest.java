package datawave.query.ranges;

import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RangeFactoryTest {
    
    @Test
    public void testBuildDocumentSpecificRange() {
        String shard = "20190314_0";
        String docId = "docId0";
        Range docRange = RangeFactory.createDocumentSpecificRange(shard, docId);
        
        String expectedEndKeyCF = docId + RangeFactory.NULL_BYTE_STRING;
        
        assertEquals(shard, docRange.getStartKey().getRow().toString());
        assertEquals(docId, docRange.getStartKey().getColumnFamily().toString());
        assertEquals(shard, docRange.getEndKey().getRow().toString());
        assertEquals(expectedEndKeyCF, docRange.getEndKey().getColumnFamily().toString());
    }
    
    @Test
    public void testBuildTldDocumentSpecificRange() {
        String shard = "20190314_0";
        String docId = "docId0";
        Range tldDocRange = RangeFactory.createTldDocumentSpecificRange(shard, docId);
        
        String expectedEndKeyCF = docId + RangeFactory.MAX_UNICODE_STRING;
        
        assertEquals(shard, tldDocRange.getStartKey().getRow().toString());
        assertEquals(docId, tldDocRange.getStartKey().getColumnFamily().toString());
        assertEquals(shard, tldDocRange.getEndKey().getRow().toString());
        assertEquals(expectedEndKeyCF, tldDocRange.getEndKey().getColumnFamily().toString());
    }
    
    @Test
    public void testBuildShardRange() {
        String shard = "20190314_0";
        Range shardRange = RangeFactory.createShardRange(shard);
        
        String expectedEndKeyRow = shard + RangeFactory.NULL_BYTE_STRING;
        
        assertEquals(shard, shardRange.getStartKey().getRow().toString());
        assertEquals(expectedEndKeyRow, shardRange.getEndKey().getRow().toString());
    }
    
    @Test
    public void testBuildDayRange() {
        String shard = "20190314";
        Range dayRange = RangeFactory.createDayRange(shard);
        
        String expectedStartKeyRow = "20190314_0";
        String expectedEndKeyRow = shard + RangeFactory.MAX_UNICODE_STRING;
        
        assertEquals(expectedStartKeyRow, dayRange.getStartKey().getRow().toString());
        assertEquals(expectedEndKeyRow, dayRange.getEndKey().getRow().toString());
    }
}
