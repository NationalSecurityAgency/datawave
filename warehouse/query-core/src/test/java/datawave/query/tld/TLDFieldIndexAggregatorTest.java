package datawave.query.tld;

import com.google.common.collect.Maps;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TLDFieldIndexAggregatorTest {
    private TLDFieldIndexAggregator aggregator;
    
    @BeforeEach
    public void setup() {
        aggregator = new TLDFieldIndexAggregator(null, null, -1);
    }
    
    @Test
    public void apply_testAggregateFilter() throws IOException {
        EventDataQueryFilter mockFilter = EasyMock.createMock(EventDataQueryFilter.class);
        
        TypeMetadata typeMetadata = new TypeMetadata();
        AttributeFactory factory = new AttributeFactory(typeMetadata);
        
        Set<String> aggregatedFields = new HashSet<>();
        aggregatedFields.add("FOO");
        
        aggregator = new TLDFieldIndexAggregator(aggregatedFields, mockFilter, -1);
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        Key fi1 = getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10);
        Key fi2 = getFi("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.1", 10);
        Key fi3 = getFi("123", "FIELD1", "VALUE3", "dataType1", "123.345.456.2", 10);
        Key fi4 = getFi("123", "FIELD1", "VALUE4", "dataType1", "123.345.456.3", 10);
        // FOO included in the filter
        Key fi5 = getFi("123", "FOO", "bar", "dataType1", "123.345.456.3", 10);
        // FOO2 not included in the filter
        Key fi6 = getFi("123", "FOO2", "bar", "dataType1", "123.345.456.3", 10);
        // key outside the range which should not be aggregated
        Key fi7 = getFi("123", "XENO", "zap", "dataType1", "234.345.456", 10);
        
        treeMap.put(fi1, new Value());
        treeMap.put(fi2, new Value());
        treeMap.put(fi3, new Value());
        treeMap.put(fi4, new Value());
        treeMap.put(fi5, new Value());
        treeMap.put(fi6, new Value());
        treeMap.put(fi7, new Value());
        
        EasyMock.expect(mockFilter.keep(EasyMock.isA(Key.class))).andReturn(true);
        
        EasyMock.replay(mockFilter);
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Document doc = new Document();
        aggregator.apply(itr, doc, factory);
        
        EasyMock.verify(mockFilter);
        
        // list of FIELD1 values to expect
        List<String> expectedFieldValues = new ArrayList<>();
        expectedFieldValues.add("VALUE1");
        expectedFieldValues.add("VALUE2");
        expectedFieldValues.add("VALUE3");
        expectedFieldValues.add("VALUE4");
        
        assertTrue(doc.get("FIELD1").isToKeep());
        Set<Attribute> attributes = ((Set<Attribute>) doc.get("FIELD1").getData());
        assertEquals(4, attributes.size());
        for (Attribute attr : attributes) {
            assertFalse(attr.isToKeep());
            assertTrue(expectedFieldValues.remove(attr.getData().toString()));
        }
        
        assertEquals(0, expectedFieldValues.size());
        // FOO kept
        assertTrue(doc.get("FOO").isToKeep());
        // FOO2 not kept
        assertFalse(doc.get("FOO2").isToKeep());
        // out of document range not included
        assertNull(doc.get("XENO"));
    }
    
    @Test
    public void apply_testNormal() throws IOException {
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.2", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.3", 10), new Value());
        
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
        aggregator = new TLDFieldIndexAggregator(null, null, 1);
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.2", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.3", 10), new Value());
        treeMap.put(getFi("123", "FIELD1", "VALUE1", "dataType1", "123.345.457", 11), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        Key result = aggregator.apply(itr);
        
        assertTrue(itr.hasTop());
        assertEquals(11, itr.getTopKey().getTimestamp());
        
        itr.seek(new Range(), null, true);
        Key result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals(11, itr.getTopKey().getTimestamp());
        assertEquals(result, result2);
        
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
    }
    
    private Key getFi(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, "fi" + Constants.NULL_BYTE_STRING + field, value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING + uid,
                        timestamp);
    }
}
