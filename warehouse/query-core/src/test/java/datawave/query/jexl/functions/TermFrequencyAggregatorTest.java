package datawave.query.jexl.functions;

import com.google.common.collect.Maps;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TermFrequencyAggregatorTest {
    private TermFrequencyAggregator aggregator;
    
    @Before
    public void setup() {
        aggregator = new TermFrequencyAggregator(null, null, -1);
    }
    
    @Test
    public void apply_buildDocNotKeep() throws IOException {
        Document doc = new Document();
        AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD2");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter();
        Set<String> blacklist = new HashSet<>();
        blacklist.add("FIELD1");
        ((EventDataQueryFieldFilter) filter).initializeBlacklist(blacklist);
        
        aggregator = new TermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);
        
        // test result key
        assertTrue(result == null);
        
        // test that the doc is empty
        assertTrue(doc.size() == 0);
        
        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().equals(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10)));
    }
    
    @Test
    public void apply_buildDocKeep() throws IOException, ParseException {
        Document doc = new Document();
        AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD1");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(JexlASTHelper.parseJexlQuery("FIELD1 == 'VALUE1'"), Collections.emptySet());
        aggregator = new TermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);
        
        // test result key
        assertTrue(result != null);
        DatawaveKey parsedResult = new DatawaveKey(result);
        assertTrue(parsedResult.getDataType().equals("dataType1"));
        assertTrue(parsedResult.getUid().equals("123.345.456"));
        assertTrue(parsedResult.getFieldName().equals("FIELD1"));
        assertTrue(parsedResult.getFieldValue().equals("VALUE1"));
        
        // test that the doc is empty
        assertTrue(doc.size() == 2);
        assertTrue(doc.get("RECORD_ID").getData().equals("123/dataType1/123.345.456"));
        assertTrue(doc.get("FIELD1").getData().toString().equals("VALUE1"));
        
        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().equals(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10)));
    }
    
    @Test
    public void apply_buildDocKeepFilteredOut() throws IOException, ParseException {
        Document doc = new Document();
        AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD2");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(JexlASTHelper.parseJexlQuery("FIELD2 == 'VALUE1'"), Collections.EMPTY_SET);
        aggregator = new TermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);
        
        // test result key
        assertTrue(result == null);
        
        // test that the doc is empty
        assertTrue(doc.size() == 0);
        
        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().equals(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10)));
    }
    
    @Test
    public void apply_testNormal() throws IOException {
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        
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
        aggregator = new TermFrequencyAggregator(null, null, 1);
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getTF("1234", "FIELD1", "VALUE1", "dataType1", "123.345.456", 7), new Value());
        
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
        
        // test a change to the datatype
        treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType2", "123.345.456", 7), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals(7, itr.getTopKey().getTimestamp());
        assertEquals(result, result2);
        
        // test a change to the uid
        treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 7), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertEquals(7, itr.getTopKey().getTimestamp());
        assertEquals(result, result2);
        
        treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertFalse(itr.hasTop());
        assertEquals(result, result2);
    }
    
    private Key getTF(String row, String field, String value, String dataType, String uid, long timestamp) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        return new Key(row, "tf", dataType + Constants.NULL_BYTE_STRING + uid + Constants.NULL_BYTE_STRING + value + Constants.NULL_BYTE_STRING + field,
                        timestamp);
    }
}
