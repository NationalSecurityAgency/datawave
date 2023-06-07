package datawave.query.iterator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.RangeProvider;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.predicate.ParentRangeProvider;
import datawave.query.predicate.TimeFilter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParentQueryIteratorTest {
    
    private static final String ID_PREFIX = "idpart1.idpart2.";
    DocumentDeserializer deserializer = null;
    
    @Before
    public void setup() {
        this.deserializer = new KryoDocumentDeserializer();
    }
    
    @Test
    public void test() throws Throwable {
        ParentQueryIterator qitr = new ParentQueryIterator();
        Map<String,String> options = Maps.newHashMap();
        
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        
        createChildren(data);
        
        options.put(QueryOptions.DISABLE_EVALUATION, "false");
        options.put(QueryOptions.QUERY, "FOO=='bars'");
        options.put(QueryOptions.TYPE_METADATA, "FOO:[test:datawave.data.type.LcNoDiacriticsType]");
        options.put(QueryOptions.REDUCED_RESPONSE, "false");
        options.put(Constants.RETURN_TYPE, "kryo");
        options.put(QueryOptions.FULL_TABLE_SCAN_ONLY, "false");
        options.put(QueryOptions.FILTER_MASKED_VALUES, "true");
        options.put(QueryOptions.TERM_FREQUENCY_FIELDS, "FOO");
        options.put(QueryOptions.INCLUDE_DATATYPE, "true");
        options.put(QueryOptions.INDEX_ONLY_FIELDS, "FOO");
        options.put(QueryOptions.START_TIME, "0");
        options.put(QueryOptions.END_TIME, Long.toString(Long.MAX_VALUE));
        options.put(QueryOptions.POSTPROCESSING_CLASSES, "");
        options.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "false");
        options.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");
        
        // the iterator will npe if these guys aren't set
        qitr.setTimeFilter(TimeFilter.alwaysTrue());
        
        qitr.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());
        qitr.seek(new Range(new Key("20121126_0", "foobar\u0000idpart1.idpart2.idpart31"), true, new Key("2121126_0", "foobar\u0000idpart1.idpart2" + "\0"),
                        false), Collections.<ByteSequence> emptySet(), false);
        
        assertTrue(qitr.hasTop());
        Key topKey = qitr.getTopKey();
        Key expectedKey = new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        assertEquals(expectedKey, topKey);
        
        Entry<Key,Document> doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        Attribute<?> recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_0/foobar/idpart1.idpart2.idpart31", recordId.getData());
        
        assertTrue(qitr.hasTop());
        qitr.next();
        expectedKey = new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        topKey = qitr.getTopKey();
        assertEquals(expectedKey, topKey);
        
        doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_0/foobar/idpart1.idpart2.idpart31", recordId.getData());
        
        qitr.next();
        
        assertTrue(qitr.hasTop());
        
        expectedKey = new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.3", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        topKey = qitr.getTopKey();
        assertEquals(expectedKey, topKey);
        
        doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_0/foobar/idpart1.idpart2.idpart31", recordId.getData());
        
        qitr.next();
        
        assertFalse(qitr.hasTop());
    }
    
    @Test
    public void testGetRangeProvider() {
        ParentQueryIterator iterator = new ParentQueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(ParentRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }
    
    private void createChildren(SortedMap<Key,Value> map) {
        long ts = QueryIteratorTest.getTimeStamp();
        
        long ts2 = ts + 10000;
        long ts3 = ts + 200123;
        
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bars\0" + "foobar\0" + ID_PREFIX + "idpart31." + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bars\0" + "foobar\0" + ID_PREFIX + "idpart31." + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bars\0" + "foobar\0" + ID_PREFIX + "idpart31." + 3, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 1, "FOO\0bars", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 1, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 2, "FOO\0bars", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 2, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 3, "FOO\0bars", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31." + 3, "BAR\0foo", ts2), new Value(new byte[0]));
        
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart32." + 4, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart32." + 5, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart32." + 6, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart32." + 4, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart32." + 5, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart32." + 5, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart32." + 6, "FOO\0bar", ts2), new Value(new byte[0]));
        
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart33." + 7, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart33." + 8, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + ID_PREFIX + "idpart33." + 9, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 7, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 7, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 8, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 8, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 9, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart33." + 9, "BAR\0foo", ts3), new Value(new byte[0]));
    }
    
    @Test
    public void testParentFiOnlyDocsAllowed() throws Throwable {
        ParentQueryIterator qitr = new ParentQueryIterator();
        Map<String,String> options = Maps.newHashMap();
        
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        
        createOrphanedChildren(data);
        
        options.put(QueryOptions.DISABLE_EVALUATION, "false");
        options.put(QueryOptions.QUERY, "FOO=='baz'");
        options.put(QueryOptions.TYPE_METADATA, "FOO:[test:datawave.data.type.LcNoDiacriticsType]");
        options.put(QueryOptions.REDUCED_RESPONSE, "false");
        options.put(Constants.RETURN_TYPE, "kryo");
        options.put(QueryOptions.FULL_TABLE_SCAN_ONLY, "false");
        options.put(QueryOptions.FILTER_MASKED_VALUES, "true");
        options.put(QueryOptions.TERM_FREQUENCY_FIELDS, "FOO");
        options.put(QueryOptions.INCLUDE_DATATYPE, "true");
        options.put(QueryOptions.INDEX_ONLY_FIELDS, "FOO");
        options.put(QueryOptions.START_TIME, "0");
        options.put(QueryOptions.END_TIME, Long.toString(Long.MAX_VALUE));
        options.put(QueryOptions.POSTPROCESSING_CLASSES, "");
        options.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "false");
        options.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");
        
        // the iterator will npe if these guys aren't set
        qitr.setTimeFilter(TimeFilter.alwaysTrue());
        
        qitr.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());
        qitr.seek(new Range(new Key("20121126_2", "foobar\u0000idpart1.idpart2.idpart34"), true, new Key("2121126_3", "foobar\u0000idpart1.idpart2.idpart35"),
                        false), Collections.<ByteSequence> emptySet(), false);
        
        assertTrue(qitr.hasTop());
        Key topKey = qitr.getTopKey();
        Key expectedKey = new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        assertEquals(expectedKey, topKey);
        
        Entry<Key,Document> doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        Attribute<?> recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_2/foobar/idpart1.idpart2.idpart36", recordId.getData());
        
        assertTrue(qitr.hasTop());
        qitr.next();
        expectedKey = new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        topKey = qitr.getTopKey();
        assertEquals(expectedKey, topKey);
        
        doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_2/foobar/idpart1.idpart2.idpart36", recordId.getData());
        
        qitr.next();
        
        assertFalse(qitr.hasTop());
    }
    
    @Test
    public void testParentNoFiOnlyDocs() throws Throwable {
        ParentQueryIterator qitr = new ParentQueryIterator();
        Map<String,String> options = Maps.newHashMap();
        
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        
        createOrphanedChildren(data);
        
        options.put(QueryOptions.DISABLE_EVALUATION, "false");
        options.put(QueryOptions.QUERY, "FOO=='baz'");
        options.put(QueryOptions.TYPE_METADATA, "FOO:[test:datawave.data.type.LcNoDiacriticsType]");
        options.put(QueryOptions.REDUCED_RESPONSE, "false");
        options.put(Constants.RETURN_TYPE, "kryo");
        options.put(QueryOptions.FULL_TABLE_SCAN_ONLY, "false");
        options.put(QueryOptions.FILTER_MASKED_VALUES, "true");
        options.put(QueryOptions.TERM_FREQUENCY_FIELDS, "FOO");
        options.put(QueryOptions.INCLUDE_DATATYPE, "true");
        options.put(QueryOptions.INDEX_ONLY_FIELDS, "FOO");
        options.put(QueryOptions.START_TIME, "0");
        options.put(QueryOptions.DISABLE_DOCUMENTS_WITHOUT_EVENTS, "true");
        options.put(QueryOptions.END_TIME, Long.toString(Long.MAX_VALUE));
        options.put(QueryOptions.POSTPROCESSING_CLASSES, "");
        options.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "false");
        options.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");
        
        // the iterator will npe if these guys aren't set
        qitr.setTimeFilter(TimeFilter.alwaysTrue());
        
        qitr.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());
        qitr.seek(new Range(new Key("20121126_2", "foobar\u0000idpart1.idpart2.idpart34"), true, new Key("2121126_3", "foobar\u0000idpart1.idpart2.idpart35"),
                        false), Collections.<ByteSequence> emptySet(), false);
        
        assertTrue(qitr.hasTop());
        Key topKey = qitr.getTopKey();
        Key expectedKey = new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        assertEquals(expectedKey, topKey);
        
        Entry<Key,Document> doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        Attribute<?> recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_2/foobar/idpart1.idpart2.idpart36", recordId.getData());
        
        assertTrue(qitr.hasTop());
        qitr.next();
        expectedKey = new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp());
        topKey = qitr.getTopKey();
        assertEquals(expectedKey, topKey);
        
        doc = deserializer.apply(Maps.immutableEntry(topKey, qitr.getTopValue()));
        
        recordId = doc.getValue().get(Document.DOCKEY_FIELD_NAME);
        if (recordId instanceof Attributes) {
            recordId = ((Attributes) recordId).getAttributes().iterator().next();
        }
        assertEquals("20121126_2/foobar/idpart1.idpart2.idpart36", recordId.getData());
        
        qitr.next();
        
        assertFalse(qitr.hasTop());
    }
    
    private void createOrphanedChildren(SortedMap<Key,Value> map) {
        long ts = QueryIteratorTest.getTimeStamp();
        
        long ts3 = ts + 200123;
        
        // scenario 1, fi keys for child docs, but on cihldren or parent
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart34." + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart34." + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart34." + 3, ts), new Value(new byte[0]));
        
        // scenario 2, fi keys for child docs, children exist but no parent
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart35." + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart35." + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart35." + 1, "FOO\0baz", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart35." + 2, "BAR\0foo", ts3), new Value(new byte[0]));
        
        // scenario 3, fi keys child docs, no children and parent exists
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart36." + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "baz\0" + "foobar\0" + ID_PREFIX + "idpart36." + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36", "FOO\0baz", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36", "BAR\0foo", ts3), new Value(new byte[0]));
    }
    
    @Test
    public void testTearDown() throws Exception {
        SortedMapIterator iter = new SortedMapIterator(QueryIteratorTest.createTestData(ID_PREFIX + "idpart3"));
        Set<Key> expectation = Sets.newHashSet(
                        new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart3" + 1, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart3" + 2, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart3" + 3, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart3" + 5, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart3" + 7, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart3" + 8, QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()),
                        new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart39", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));
        
        Map<String,String> options = Maps.newHashMap();
        
        options.put(QueryOptions.DISABLE_EVALUATION, "false");
        options.put(QueryOptions.QUERY, "FOO == 'bar' && BAR == 'foo'");
        options.put(QueryOptions.TYPE_METADATA, "FOO:[test:datawave.data.type.LcNoDiacriticsType]");
        options.put(QueryOptions.REDUCED_RESPONSE, "true");
        options.put(Constants.RETURN_TYPE, "kryo");
        options.put(QueryOptions.FULL_TABLE_SCAN_ONLY, "false");
        options.put(QueryOptions.FILTER_MASKED_VALUES, "true");
        options.put(QueryOptions.INCLUDE_DATATYPE, "true");
        options.put(QueryOptions.INDEX_ONLY_FIELDS, "");
        options.put(QueryOptions.START_TIME, "0");
        options.put(QueryOptions.END_TIME, Long.toString(Long.MAX_VALUE));
        options.put(QueryOptions.POSTPROCESSING_CLASSES, "");
        options.put(QueryOptions.INCLUDE_GROUPING_CONTEXT, "false");
        options.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "false");
        
        ParentQueryIterator qi = new ParentQueryIterator();
        
        qi.init(iter, options, new SourceManagerTest.MockIteratorEnvironment());
        
        qi.seek(new Range(new Key("20121126"), false, new Key("20121127"), false), Collections.<ByteSequence> emptyList(), false);
        
        while (qi.hasTop()) {
            System.out.println("begin loop1: " + expectation);
            
            Key tk = qi.getTopKey();
            assertTrue("Could not remove " + tk + " from " + expectation.size(), expectation.remove(tk));
            
            String cf = tk.getColumnFamily().toString();
            
            if (cf.contains("idpart35")) {
                break;
            }
            
            qi.next();
            
            System.out.println("ender loop1: " + expectation);
        }
        
        qi = new ParentQueryIterator();
        
        qi.init(iter, options, new SourceManagerTest.MockIteratorEnvironment());
        
        qi.seek(new Range(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart35"), false, new Key("20121127"), false),
                        Collections.<ByteSequence> emptyList(), false);
        
        while (qi.hasTop()) {
            Key tk = qi.getTopKey();
            assertTrue("Could not remove " + tk + " from " + expectation, expectation.remove(tk));
            qi.next();
        }
        
        assertTrue("Still had expected keys: " + expectation, expectation.isEmpty());
    }
}
