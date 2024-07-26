package datawave.query.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.Test;

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

public class ParentQueryIteratorTest {

    private static final String ID_PREFIX = "idpart1.idpart2.";
    private final DocumentDeserializer deserializer = new KryoDocumentDeserializer();

    @Test
    public void testGetRangeProvider() {
        ParentQueryIterator iterator = new ParentQueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(ParentRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }

    @Test
    public void testNormalIteration() throws Throwable {
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        createChildren(data);

        Map<String,String> options = getOptions();
        options.put(QueryOptions.QUERY, "FOO=='bars'");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");

        ParentQueryIterator iter = getIterator();
        iter.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());

        Key start = new Key("20121126_0", "foobar\u0000idpart1.idpart2.idpart31");
        Key stop = new Key("2121126_0", "foobar\u0000idpart1.idpart2" + "\0");
        Range range = new Range(start, true, stop, false);

        String expectedRecordId = "20121126_0/foobar/idpart1.idpart2.idpart31";

        Set<Key> expectedTKs = new HashSet<>();
        expectedTKs.add(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));
        expectedTKs.add(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));
        expectedTKs.add(new Key("20121126_0", "foobar\0" + ID_PREFIX + "idpart31.3", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));

        driveIterator(iter, range, expectedRecordId, expectedTKs);
    }

    @Test
    public void testParentFiOnlyDocsAllowed() throws Throwable {
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        createOrphanedChildren(data);

        Map<String,String> options = getOptions();
        options.put(QueryOptions.QUERY, "FOO=='baz'");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");

        ParentQueryIterator iter = getIterator();
        iter.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());

        Key start = new Key("20121126_2", "foobar\u0000idpart1.idpart2.idpart34");
        Key stop = new Key("2121126_3", "foobar\u0000idpart1.idpart2.idpart35");
        Range range = new Range(start, true, stop, false);

        String expectedRecordId = "20121126_2/foobar/idpart1.idpart2.idpart36";

        Set<Key> expectedTKs = new HashSet<>();
        expectedTKs.add(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));
        expectedTKs.add(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));

        driveIterator(iter, range, expectedRecordId, expectedTKs);
    }

    @Test
    public void testParentNoFiOnlyDocs() throws Throwable {
        SortedMap<Key,Value> data = QueryIteratorTest.createTestData(ID_PREFIX + "idpart3");
        createOrphanedChildren(data);

        Map<String,String> options = getOptions();

        options.put(QueryOptions.QUERY, "FOO=='baz'");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "true");

        ParentQueryIterator iter = getIterator();
        iter.init(new SortedMapIterator(data), options, new SourceManagerTest.MockIteratorEnvironment());

        Key start = new Key("20121126_2", "foobar\u0000idpart1.idpart2.idpart34");
        Key stop = new Key("2121126_3", "foobar\u0000idpart1.idpart2.idpart35");
        Range range = new Range(start, true, stop, false);

        String expectedRecordId = "20121126_2/foobar/idpart1.idpart2.idpart36";

        Set<Key> expectedTKs = new HashSet<>();
        expectedTKs.add(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.1", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));
        expectedTKs.add(new Key("20121126_2", "foobar\0" + ID_PREFIX + "idpart36.2", QueryIteratorTest.DEFAULT_CQ, "", QueryIteratorTest.getTimeStamp()));

        driveIterator(iter, range, expectedRecordId, expectedTKs);
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

        Map<String,String> options = getOptions();
        options.put(QueryOptions.QUERY, "FOO == 'bar' && BAR == 'foo'");
        options.put(QueryOptions.INDEX_ONLY_FIELDS, "");
        options.put(QueryOptions.NON_INDEXED_DATATYPES, "");
        options.put(QueryOptions.CONTAINS_INDEX_ONLY_TERMS, "false");

        ParentQueryIterator qi = new ParentQueryIterator();

        qi.init(iter, options, new SourceManagerTest.MockIteratorEnvironment());

        qi.seek(new Range(new Key("20121126"), false, new Key("20121127"), false), Collections.emptyList(), false);

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

        qi.seek(new Range(new Key("20121126_1", "foobar\0" + ID_PREFIX + "idpart35"), false, new Key("20121127"), false), Collections.emptyList(), false);

        while (qi.hasTop()) {
            Key tk = qi.getTopKey();
            assertTrue("Could not remove " + tk + " from " + expectation, expectation.remove(tk));
            qi.next();
        }

        assertTrue("Still had expected keys: " + expectation, expectation.isEmpty());
    }

    private void driveIterator(ParentQueryIterator iter, Range range, String expectedRecordId, Set<Key> expectedTopKeys) throws IOException {

        iter.seek(range, Collections.emptySet(), false);

        while (iter.hasTop()) {
            Key tk = iter.getTopKey();
            assertTrue("unexpected top key was found: " + tk.toStringNoTime(), expectedTopKeys.remove(tk));

            Document d = deserializer.apply(Maps.immutableEntry(tk, iter.getTopValue())).getValue();
            assertEquals(expectedRecordId, getRecordId(d));

            iter.next();
        }

        assertFalse(iter.hasTop());
        assertTrue("expected top keys remain: " + expectedTopKeys, expectedTopKeys.isEmpty());
    }

    private String getRecordId(Document d) {
        Attribute<?> attr = d.get(Document.DOCKEY_FIELD_NAME);
        if (attr instanceof Attributes) {
            attr = ((Attributes) attr).getAttributes().iterator().next();
        }
        return String.valueOf(attr.getData());
    }

    private ParentQueryIterator getIterator() {
        ParentQueryIterator iter = new ParentQueryIterator();
        iter.setTimeFilter(TimeFilter.alwaysTrue());
        return iter;
    }

    private Map<String,String> getOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QueryOptions.DISABLE_EVALUATION, "false");
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

        return options;
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
}
