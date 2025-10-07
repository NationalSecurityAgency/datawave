package datawave.query.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.data.hash.UID;
import datawave.query.iterator.QueryOptions;
import datawave.query.util.Tuple3;

/**
 * Tests for the {@link DescendantCountFunction}
 */
public class DescendantCountFunctionTest {

    private final Value emptyValue = new Value();
    private final Long ts = 1L;
    private final String row = "20240314_12";
    private final String id = UUID.randomUUID().toString();
    private final String uid = UID.builder().newId(new Date()).getBaseUid();

    private final RangeProvider rangeProvider = new TLDRangeProvider();

    private final Collection<ByteSequence> columnFamilies = Lists.newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));
    private final String CHILD_COUNT = "CHILD_COUNT";
    private final String DESCENDANT_COUNT = "DESCENDANT_COUNT";

    private final Map<String,String> options = new HashMap<>();

    @BeforeEach
    public void before() {
        options.clear();
    }

    @Test
    public void testEmptyInputs() throws IOException {
        DescendantCountFunction function = new DescendantCountFunction();
        function.init(getSource(), null, null);

        DescendantCount result = function.apply(null);
        assertEmptyCountResult(result);

        result = function.apply(new Tuple3<>(null, null, null));
        assertEmptyCountResult(result);
    }

    @Test
    public void testEvenScanParentDocument() throws Exception {
        Key key = new Key(row, "datatype\0" + uid);

        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(-1);
        //  @formatter:on
    }

    @Test
    public void testEventScanLowCountChild() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(-1);
        //  @formatter:on
    }

    @Test
    public void testEventScanLowCountLeaf() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1.1");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(false)
                        .assertCount(CHILD_COUNT,0)
                        .assertFirstGenCount(0)
                        .assertAllGenCount(-1);
        //  @formatter:on
    }

    @Test
    public void testEventScanHighCountChild() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".2");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,100)
                        .assertFirstGenCount(100)
                        .assertAllGenCount(-1);
        //  @formatter:on
    }

    // scan the field index

    @Test
    public void testFieldIndexScanParentDocument() throws Exception {
        Key key = new Key(row, "datatype\0" + uid);
        configureFieldName("ID");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(104);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanLowCountChild() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1");
        configureFieldName("ID");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(2);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanLowCountLeaf() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1.1");
        configureFieldName("ID");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(false)
                        .assertCount(CHILD_COUNT,0)
                        .assertFirstGenCount(0)
                        .assertAllGenCount(0);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanHighCountChild() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".2");
        configureFieldName("ID");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,100)
                        .assertFirstGenCount(100)
                        .assertAllGenCount(100);
        //  @formatter:on
    }

    // field index scanning with skip limit

    @Test
    public void testFieldIndexScanParentDocumentWithSkipLimit() throws Exception {
        Key key = new Key(row, "datatype\0" + uid);
        configureFieldName("ID");
        configureSkipThreshold("25");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(29);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanLowCountChildWithSkipLimit() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1");
        configureFieldName("ID");
        configureSkipThreshold("25");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(2);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanHighCountChildWithSkipLimit() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".2");
        configureFieldName("ID");
        configureSkipThreshold("25");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,100)
                        .assertFirstGenCount(100)
                        .assertAllGenCount(100);
        //  @formatter:on
    }

    // test field index scan with regex delimiter
    @Test
    public void testFieldIndexScanParentDocumentWithRegexDelimiter() throws Exception {
        Key key = new Key(row, "datatype\0" + uid);
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(104);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanLowCountChildWithRegexDelimiter() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1");
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,2)
                        .assertFirstGenCount(2)
                        .assertAllGenCount(2);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanHighCountChildWithRegexDelimiter() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".2");
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(CHILD_COUNT,100)
                        .assertFirstGenCount(100)
                        .assertAllGenCount(100);
        //  @formatter:on
    }

    // test field index scan with other counts

    @Test
    public void testFieldIndexScanParentDocumentWithOtherCounts() throws Exception {
        Key key = new Key(row, "datatype\0" + uid);
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        configureAllDescendants("true");
        configureImmediateChildren("false");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(DESCENDANT_COUNT, 104)
                        .assertFirstGenCount(0)
                        .assertAllGenCount(104);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanLowCountChildWithOtherCounts() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".1");
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        configureAllDescendants("true");
        configureImmediateChildren("false");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(DESCENDANT_COUNT, 2)
                        .assertFirstGenCount(0)
                        .assertAllGenCount(2);
        //  @formatter:on
    }

    @Test
    public void testFieldIndexScanHighCountChildWithOtherCounts() throws Exception {
        Key key = new Key(row, "datatype\0" + uid + ".2");
        configureFieldName("ID");
        configureDelimiter("-att-[0-9]*");
        configureAllDescendants("true");
        configureImmediateChildren("false");
        //  @formatter:off
        driveAggregation(key)
                        .assertHasDescendents(true)
                        .assertCount(DESCENDANT_COUNT,100)
                        .assertFirstGenCount(0)
                        .assertAllGenCount(100);
        //  @formatter:on
    }

    /**
     * Drive an aggregation using a {@link KeyToDocumentData} configured with a {@link TLDEquality}
     *
     * @param key
     *            the document key
     */
    private Result driveAggregation(Key key) throws Exception {
        return driveAggregation(key, options);
    }

    /**
     * Drive an aggregation using a {@link KeyToDocumentData} configured with a {@link TLDEquality}
     *
     * @param key
     *            the document key
     * @param options
     *            an options map for the {@link DescendantCountFunction}
     */
    private Result driveAggregation(Key key, Map<String,String> options) throws Exception {

        SortedKeyValueIterator<Key,Value> source = getSource();
        Range seekRange = getSeekRange(key);

        DescendantCountFunction function = new DescendantCountFunction();
        function.init(source, options, null);

        KeyToDocumentData k2d = new KeyToDocumentData(source, new TLDEquality(), null, false, false);

        source.seek(seekRange, columnFamilies, false);
        List<Map.Entry<Key,Value>> result = k2d.collectDocumentAttributes(key, new HashSet<>(), rangeProvider.getRange(key));

        DescendantCount count = function.apply(new Tuple3<>(seekRange, key, result));
        return new Result(count);
    }

    private Range getSeekRange(Key key) {
        Key stopKey = new Key(key.getRow().toString(), key.getColumnFamily().toString() + '\uffff');
        return new Range(key, false, stopKey, false);
    }

    private void assertEmptyCountResult(DescendantCount count) {
        assertNotNull(count);
        assertTrue(count.getKeys().isEmpty());
    }

    private SortedKeyValueIterator<Key,Value> getSource() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // parent document - event
        createEventKey(data, "ID", id, uid);
        createEventKey(data, "FIELD_A", "foo", uid);
        createEventKey(data, "FIELD_A", "bar", uid);
        createEventKey(data, "FIELD_B", "baz", uid);

        // parent document - field index
        createFiKey(data, "ID", id, uid);
        createFiKey(data, "FIELD_A", "foo", uid);
        createFiKey(data, "FIELD_A", "bar", uid);
        createFiKey(data, "FIELD_B", "baz", uid);

        // child document - event
        createEventKey(data, "ID", id + "-att-1", uid + ".1");
        createEventKey(data, "FIELD_A", "foo", uid + ".1");
        createEventKey(data, "FIELD_A", "bar", uid + ".1");
        createEventKey(data, "FIELD_B", "baz", uid + ".1");

        // child document - field index
        createFiKey(data, "ID", id + "-att-1", uid + ".1");
        createFiKey(data, "FIELD_A", "foo", uid + ".1");
        createFiKey(data, "FIELD_A", "bar", uid + ".1");
        createFiKey(data, "FIELD_B", "baz", uid + ".1");

        // first child has 2 children
        for (int i = 1; i <= 2; i++) {
            createEventKey(data, "ID", id + "-att-1-att-" + i, uid + ".1." + i);
            createEventKey(data, "FIELD_A", "foo", uid + ".1." + i);
            createEventKey(data, "FIELD_A", "bar", uid + ".1." + i);
            createEventKey(data, "FIELD_B", "baz", uid + ".1." + i);

            createFiKey(data, "ID", id + "-att-1-att-" + i, uid + ".1." + i);
            createFiKey(data, "FIELD_A", "foo", uid + ".1." + i);
            createFiKey(data, "FIELD_A", "bar", uid + ".1." + i);
            createFiKey(data, "FIELD_B", "baz", uid + ".1." + i);
        }

        // second child - event
        createEventKey(data, "ID", id + "-att-2", uid + ".2");
        createEventKey(data, "FIELD_A", "foo", uid + ".2");
        createEventKey(data, "FIELD_A", "bar", uid + ".2");
        createEventKey(data, "FIELD_B", "baz", uid + ".2");

        // second child - field index
        createFiKey(data, "ID", id + "-att-2", uid + ".2");
        createFiKey(data, "FIELD_A", "foo", uid + ".2");
        createFiKey(data, "FIELD_A", "bar", uid + ".2");
        createFiKey(data, "FIELD_B", "baz", uid + ".2");

        // second child has 100 children
        for (int i = 1; i <= 100; i++) {
            // grandchild, different branch - event
            createEventKey(data, "ID", id + "-att-2-att-" + i, uid + ".2." + i);
            createEventKey(data, "FIELD_A", "foo", uid + ".2." + i);
            createEventKey(data, "FIELD_A", "bar", uid + ".2." + i);
            createEventKey(data, "FIELD_B", "baz", uid + ".2." + i);

            // grandchild, different branch - field index
            createFiKey(data, "ID", id + "-att-2-att-" + i, uid + ".2." + i);
            createFiKey(data, "FIELD_A", "foo", uid + ".2." + i);
            createFiKey(data, "FIELD_A", "bar", uid + ".2." + i);
            createFiKey(data, "FIELD_B", "baz", uid + ".2." + i);
        }

        return new SortedMapIterator(data);
    }

    private void createEventKey(SortedMap<Key,Value> data, String field, String value, String uid) {
        data.put(new Key(row, "datatype\0" + uid, field + "\0" + value, ts), emptyValue);
    }

    private void createFiKey(SortedMap<Key,Value> data, String field, String value, String uid) {
        data.put(new Key(row, "fi\0" + field, value + "\0datatype\0" + uid, ts), emptyValue);
    }

    private static class Result {

        private final DescendantCount count;

        public Result(DescendantCount count) {
            this.count = count;
        }

        private Result assertHasDescendents(boolean expected) {
            assertNotNull(count, "Expected DescendantCount to be non-null");
            assertEquals(expected, count.hasDescendants());
            return this;
        }

        private Result assertFirstGenCount(int expected) {
            assertNotNull(count, "Expected DescendantCount to be non-null");
            assertEquals(expected, count.getFirstGenerationCount());
            return this;
        }

        private Result assertAllGenCount(int expected) {
            assertNotNull(count, "Expected DescendantCount to be non-null");
            assertEquals(expected, count.getAllGenerationsCount());
            return this;
        }

        /**
         * Assert the expected count
         *
         * @param expected
         *            the expected descendent count
         */
        private Result assertCount(String prefix, int expected) {
            assertNotNull(count, "Expected DescendantCount to be non-null");
            List<Key> keys = count.getKeys();

            if (expected == 0) {
                assertTrue(keys.isEmpty(), "Expected an empty list of keys due to expected count of zero");
                return this;
            }

            assertFalse(keys.isEmpty(), "Expected a non-empty list of keys");
            String columnQualifier = keys.get(0).getColumnQualifier().toString();
            assertFalse(columnQualifier.isEmpty(), "Expected a non-empty column qualifier");
            String[] parts = columnQualifier.split("\0");
            assertEquals(prefix, parts[0]);
            assertEquals(expected, Integer.parseInt(parts[1]));
            return this;
        }
    }

    private void configureFieldName(String fieldName) {
        options.put(QueryOptions.CHILD_COUNT_INDEX_FIELDNAME, fieldName);
    }

    private void configureSkipThreshold(String threshold) {
        options.put(QueryOptions.CHILD_COUNT_INDEX_SKIP_THRESHOLD, threshold);
    }

    private void configureDelimiter(String delimiter) {
        options.put(QueryOptions.CHILD_COUNT_INDEX_DELIMITER, delimiter);
    }

    private void configureAllDescendants(String value) {
        options.put(QueryOptions.CHILD_COUNT_OUTPUT_ALL_DESCDENDANTS, value);
    }

    private void configureImmediateChildren(String value) {
        options.put(QueryOptions.CHILD_COUNT_OUTPUT_IMMEDIATE_CHILDREN, value);
    }
}
