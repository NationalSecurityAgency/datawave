package datawave.query.jexl.functions;

import com.google.common.collect.Maps;
import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdentityAggregatorTest {
    private IdentityAggregator aggregator;
    
    @Before
    public void setup() {
        aggregator = new IdentityAggregator();
    }
    
    @Test
    public void apply_testNormal() throws IOException {
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        Key result = aggregator.apply(itr);
        
        assertFalse(itr.hasTop());
        
        itr.seek(new Range(), null, true);
        Key result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertFalse(itr.hasTop());
        assertEquals(result, result2);
    }
    
    @Test
    public void apply_testSeek() throws IOException {
        aggregator = new IdentityAggregator(null, null, 1);
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getFi("1234", "FIELD1", "VALUE1", "dataType1", "123.345.456", 7), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        Key result = aggregator.apply(itr);
        
        assertTrue(itr.hasTop());
        assertEquals("1234", itr.getTopKey().getRow().toString());
        
        itr.seek(new Range(), null, true);
        Key result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals("1234", itr.getTopKey().getRow().toString());
        assertEquals(result, result2);
        
        // NOTE: the FIELD/VALUE are not verified as the same as part of the IdentityAggregator, so it will not be tested
        // here. In a real utilization the expectation is that there will be keys with different dataType/uids prior
        // to ever having a field/value change so the aggregator for efficiency reasons does not address this.
        
        // test a change to the datatype
        treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType2", "123.345.456", 7), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals(7, itr.getTopKey().getTimestamp());
        assertEquals(result, result2);
        
        // test a change to the uid
        treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 7), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals(7, itr.getTopKey().getTimestamp());
        assertEquals(result, result2);
    }
    
    private Key getFi(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, "fi" + Constants.NULL_BYTE_STRING + field, value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING + uid,
                        timestamp);
    }
}
