package datawave.query.tld;

import com.google.common.collect.Maps;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TLDTermFrequencyAggregatorTest {
    private TLDTermFrequencyAggregator aggregator;
    
    @Before
    public void setup() {
        aggregator = new TLDTermFrequencyAggregator(null, null, -1);
    }
    
    @Test
    public void apply_buildDocNotKeep() throws IOException, ParseException {
        Document doc = new Document();
        AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.1", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.2", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.3", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.4", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.5", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.6", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.7", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.8", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.9", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.10.1", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE2", "dataType1", "123.345.456.11.1.1", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD2");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(JexlASTHelper.parseJexlQuery("FIELD2 == 'abc'"), Collections.emptySet());
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
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
        treeMap.put(getTF("123", "FIELD2", "VALUE2", "dataType1", "123.345.456.1", 10), new Value());
        treeMap.put(getTF("123", "FIELD3", "VALUE3", "dataType1", "123.345.456.2", 10), new Value());
        treeMap.put(getTF("123", "FIELD4", "VALUE4", "dataType1", "123.345.456.3", 10), new Value());
        treeMap.put(getTF("123", "FIELD5", "VALUE5", "dataType1", "123.345.456.4", 10), new Value());
        treeMap.put(getTF("123", "FIELD6", "VALUE6", "dataType1", "123.345.456.5", 10), new Value());
        treeMap.put(getTF("123", "FIELD7", "VALUE7", "dataType1", "123.345.456.6", 10), new Value());
        treeMap.put(getTF("123", "FIELD8", "VALUE8", "dataType1", "123.345.456.7", 10), new Value());
        treeMap.put(getTF("123", "FIELD9", "VALUE9", "dataType1", "123.345.456.8", 10), new Value());
        treeMap.put(getTF("123", "FIELD10", "VALUE10", "dataType1", "123.345.456.9", 10), new Value());
        treeMap.put(getTF("123", "FIELD2", "VALUE11", "dataType1", "123.345.456.10.1", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE12", "dataType1", "123.345.456.11.1.1", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "123.345.457", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD1");
        keepFields.add("FIELD2");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(JexlASTHelper.parseJexlQuery("FIELD1 == 'VALUE1' && FIELD2 == 'VALUE2'"),
                        Collections.emptySet());
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);
        
        // test result key
        assertTrue(result != null);
        DatawaveKey parsedResult = new DatawaveKey(result);
        assertTrue(parsedResult.getDataType().equals("dataType1"));
        assertTrue(parsedResult.getUid().equals("123.345.456"));
        assertTrue(parsedResult.getFieldName(), parsedResult.getFieldName().equals("FIELD1"));
        assertTrue(parsedResult.getFieldValue().equals("VALUE1"));
        
        // test that the doc is empty
        assertTrue(doc.size() == 5);
        assertTrue(doc.get("RECORD_ID").getData().equals("123/dataType1/123.345.456"));
        assertTrue(((Set<TypeAttribute>) doc.get("FIELD1").getData()).size() == 2);
        Iterator<TypeAttribute> i = ((Set<TypeAttribute>) doc.get("FIELD1").getData()).iterator();
        List<String> expected = new ArrayList<>();
        expected.add("VALUE1");
        expected.add("VALUE12");
        while (i.hasNext()) {
            TypeAttribute ta = i.next();
            assertTrue(ta.isToKeep());
            assertTrue(expected.remove(ta.getData().toString()));
        }
        assertTrue(expected.size() == 0);
        assertTrue(((Set<TypeAttribute>) doc.get("FIELD2").getData()).size() == 2);
        i = ((Set<TypeAttribute>) doc.get("FIELD2").getData()).iterator();
        expected = new ArrayList<>();
        expected.add("VALUE2");
        expected.add("VALUE11");
        while (i.hasNext()) {
            TypeAttribute ta = i.next();
            assertTrue(ta.isToKeep());
            assertTrue(expected.remove(ta.getData().toString()));
        }
        assertTrue(expected.size() == 0);
        
        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().equals(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "123.345.457", 10)));
    }
    
    @Test
    public void apply_buildDocOnlyKeepToKeep() throws IOException, ParseException {
        Document doc = new Document();
        AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());
        
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 10), new Value());
        treeMap.put(getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        
        Set<String> keepFields = new HashSet<>();
        keepFields.add("FIELD2");
        
        EventDataQueryFilter filter = new EventDataQueryFieldFilter(JexlASTHelper.parseJexlQuery("FIELD2 == 'VALUE1'"), Collections.emptySet());
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
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
        assertTrue(result.equals(result2));
    }
    
    @Test
    public void apply_testSeek() throws IOException {
        aggregator = new TLDTermFrequencyAggregator(null, null, 1);
        TreeMap<Key,Value> treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getTF("1234", "FIELD1", "VALUE1", "dataType1", "123.345.456", 7), new Value());
        
        SortedKeyValueIterator<Key,Value> itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        Key result = aggregator.apply(itr);
        
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().getRow().toString().equals("1234"));
        
        itr.seek(new Range(), null, true);
        Key result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().getRow().toString().equals("1234"));
        assertTrue(result.equals(result2));
        
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
        assertTrue(itr.getTopKey().getTimestamp() == 7);
        assertTrue(result.equals(result2));
        
        // test a change to the uid
        treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 9), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 8), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456.1", 7), new Value());
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "432.345.456", 6), new Value());
        
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertTrue(itr.hasTop());
        assertTrue(itr.getTopKey().getTimestamp() == 6);
        assertTrue(result.equals(result2));
        
        treeMap = Maps.newTreeMap();
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "123.345.456", 10), new Value());
        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);
        
        assertFalse(itr.hasTop());
        assertTrue(result.equals(result2));
    }
    
    private Key getTF(String row, String field, String value, String dataType, String uid, long timestamp) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        return new Key(row, "tf", dataType + Constants.NULL_BYTE_STRING + uid + Constants.NULL_BYTE_STRING + value + Constants.NULL_BYTE_STRING + field,
                        timestamp);
    }
    
}
