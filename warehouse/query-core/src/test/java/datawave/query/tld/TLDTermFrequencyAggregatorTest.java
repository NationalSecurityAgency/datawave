package datawave.query.tld;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.predicate.EventDataQueryFieldFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;

public class TLDTermFrequencyAggregatorTest {
    private TLDTermFrequencyAggregator aggregator;

    @Before
    public void setup() {
        aggregator = new TLDTermFrequencyAggregator(null, null, -1);
    }

    @Test
    public void apply_buildDocNotKeep() throws IOException {
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

        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Collections.singleton("FIELD2"));
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);

        // test result key
        assertNull(result);

        // test that the doc is empty
        assertEquals(0, doc.size());

        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertEquals(itr.getTopKey(), getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10));
    }

    @Test
    public void apply_buildDocKeep() throws IOException {
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

        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Set.of("FIELD1", "FIELD2"));
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);

        // test result key
        assertNotNull(result);
        DatawaveKey parsedResult = new DatawaveKey(result);
        assertEquals("dataType1", parsedResult.getDataType());
        assertEquals("123.345.456", parsedResult.getUid());
        assertEquals(parsedResult.getFieldName(), "FIELD1", parsedResult.getFieldName());
        assertEquals("VALUE1", parsedResult.getFieldValue());

        // test that the doc is empty
        assertEquals(5, doc.size());
        assertEquals("123/dataType1/123.345.456", doc.get("RECORD_ID").getData());
        assertEquals(2, ((Set<TypeAttribute>) doc.get("FIELD1").getData()).size());
        Iterator<TypeAttribute> i = ((Set<TypeAttribute>) doc.get("FIELD1").getData()).iterator();
        List<String> expected = new ArrayList<>();
        expected.add("VALUE1");
        expected.add("VALUE12");
        while (i.hasNext()) {
            TypeAttribute ta = i.next();
            assertTrue(ta.isToKeep());
            assertTrue(expected.remove(ta.getData().toString()));
        }
        assertEquals(0, expected.size());
        assertEquals(2, ((Set<TypeAttribute>) doc.get("FIELD2").getData()).size());
        i = ((Set<TypeAttribute>) doc.get("FIELD2").getData()).iterator();
        expected = new ArrayList<>();
        expected.add("VALUE2");
        expected.add("VALUE11");
        while (i.hasNext()) {
            TypeAttribute ta = i.next();
            assertTrue(ta.isToKeep());
            assertTrue(expected.remove(ta.getData().toString()));
        }
        assertEquals(0, expected.size());

        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertEquals(itr.getTopKey(), getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "123.345.457", 10));
    }

    @Test
    public void apply_buildDocOnlyKeepToKeep() throws IOException {
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

        EventDataQueryFilter filter = new EventDataQueryFieldFilter().withFields(Collections.singleton("FIELD2"));
        aggregator = new TLDTermFrequencyAggregator(keepFields, filter, -1);
        Key result = aggregator.apply(itr, doc, attributeFactory);

        // test result key
        assertNull(result);

        // test that the doc is empty
        assertEquals(0, doc.size());

        // test that the iterator is in the correct position
        assertTrue(itr.hasTop());
        assertEquals(itr.getTopKey(), getTF("123", "NEXT_DOC_FIELD", "VALUE1", "dataType1", "124.345.456", 10));
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
        treeMap.put(getTF("123", "FIELD1", "VALUE1", "dataType1", "432.345.456", 6), new Value());

        itr = new SortedMapIterator(treeMap);
        itr.seek(new Range(), null, true);
        result2 = aggregator.apply(itr, new Range(), null, false);

        assertTrue(itr.hasTop());
        assertEquals(6, itr.getTopKey().getTimestamp());
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

    @Test
    public void testParsePointer() {
        Key tldTfKey = new Key("row", "tf", "datatype\0parent.document.id\0value\0FIELD");
        Key childTfKey = new Key("row", "tf", "datatype\0parent.document.id.child.grandchild\0value\0FIELD");

        ByteSequence expected = new ArrayByteSequence("datatype\0parent.document.id".getBytes());

        assertEquals(expected, aggregator.parsePointer(tldTfKey.getColumnQualifierData()));
        assertEquals(expected, aggregator.parsePointer(childTfKey.getColumnQualifierData()));
    }

}
