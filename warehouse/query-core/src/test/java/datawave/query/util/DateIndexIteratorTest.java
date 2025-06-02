package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;

/**
 * Tests for the {@link DateIndexIterator}
 */
public class DateIndexIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(DateIndexIteratorTest.class);

    private final SortedMap<Key,Value> data = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();

    private MockIteratorEnvironment env;

    private int expectedNextCount = 0;
    private int expectedSeekCount = 0;

    @BeforeEach
    public void setup() {
        data.clear();
        options.clear();
        expectedNextCount = 0;
        expectedSeekCount = 0;
        env = new MockIteratorEnvironment();
    }

    @Test
    public void testSimpleScan() throws IOException {
        write("20250404", "LOADED", "20250403", "datatype-a", "FIELD_A", 4, 5, 6);
        withExpectedNextSeekCounts(2, 1);
        assertExpected("20250404", "LOADED");
    }

    @Test
    public void testMultipleLoadedDates() throws IOException {
        write("20250404", "LOADED", "20250402", "datatype-a", "FIELD_A", 1, 2, 3);
        write("20250404", "LOADED", "20250403", "datatype-a", "FIELD_A", 4, 5, 6);
        write("20250404", "LOADED", "20250404", "datatype-a", "FIELD_A", 7, 8, 9);
        withExpectedNextSeekCounts(4, 1);
        assertExpected("20250404", "LOADED");
    }

    @Test
    public void testMultipleDatatypes() throws IOException {
        write("20250404", "LOADED", "20250402", "datatype-a", "FIELD_A", 1, 2, 3);
        write("20250404", "LOADED", "20250403", "datatype-a", "FIELD_A", 1, 2, 3);
        write("20250404", "LOADED", "20250404", "datatype-a", "FIELD_A", 1, 2, 3);

        write("20250404", "LOADED", "20250402", "datatype-b", "FIELD_A", 4, 5, 6);
        write("20250404", "LOADED", "20250403", "datatype-b", "FIELD_A", 4, 5, 6);
        write("20250404", "LOADED", "20250404", "datatype-b", "FIELD_A", 4, 5, 6);

        write("20250404", "LOADED", "20250402", "datatype-c", "FIELD_A", 7, 8, 9);
        write("20250404", "LOADED", "20250403", "datatype-c", "FIELD_A", 7, 8, 9);
        write("20250404", "LOADED", "20250404", "datatype-c", "FIELD_A", 7, 8, 9);

        withExpectedNextSeekCounts(10, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-a");
        withExpectedNextSeekCounts(4, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-b");
        withExpectedNextSeekCounts(4, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-c");
        withExpectedNextSeekCounts(4, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-a", "datatype-b");
        withExpectedNextSeekCounts(7, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-a", "datatype-c");
        withExpectedNextSeekCounts(7, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-b", "datatype-c");
        withExpectedNextSeekCounts(7, 1);
        assertExpected("20250404", "LOADED");

        withDatatypeFilter("datatype-a", "datatype-b", "datatype-c");
        withExpectedNextSeekCounts(10, 1);
        assertExpected("20250404", "LOADED");
    }

    @Test
    public void testRestrictScanRangeByDate() throws IOException {
        write("20250401", "LOADED", "20250401", "datatype-a", "FIELD_A", 1);
        write("20250401", "LOADED", "20250402", "datatype-a", "FIELD_A", 2);
        write("20250401", "LOADED", "20250403", "datatype-a", "FIELD_A", 3);

        write("20250402", "LOADED", "20250401", "datatype-a", "FIELD_A", 4);
        write("20250402", "LOADED", "20250402", "datatype-a", "FIELD_A", 5);
        write("20250402", "LOADED", "20250403", "datatype-a", "FIELD_A", 6);

        write("20250403", "LOADED", "20250401", "datatype-a", "FIELD_A", 7);
        write("20250403", "LOADED", "20250402", "datatype-a", "FIELD_A", 8);
        write("20250403", "LOADED", "20250403", "datatype-a", "FIELD_A", 9);

        // full scan range
        withExpectedNextSeekCounts(10, 1);
        assertExpected("20250401", "20250403", "LOADED");

        // full scan range, restrict to single day in the column qualifier
        for (String date : List.of("20250401", "20250402", "20250403")) {
            withStartEndDate(date, date);
            withExpectedNextSeekCounts(4, 1);
            assertExpected("20250401", "20250403", "LOADED");
        }
    }

    @Test
    public void testFieldFilter() throws IOException {
        write("20250401", "LOADED", "20250401", "datatype-a", "FIELD_A", 1);
        write("20250401", "LOADED", "20250402", "datatype-a", "FIELD_B", 2);
        write("20250401", "LOADED", "20250403", "datatype-a", "FIELD_C", 3);

        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C")) {
            withExpectedNextSeekCounts(2, 1);
            withField(field);
            assertExpected("20250401", "LOADED");
        }
    }

    @Test
    public void testWithTimeTravel() throws IOException {
        write("20250403", "ACTIVITY", "20250401", "datatype-a", "FIELD_A", 1);
        write("20250403", "ACTIVITY", "20250402", "datatype-a", "FIELD_A", 2);
        write("20250403", "ACTIVITY", "20250403", "datatype-a", "FIELD_A", 3);

        withDatatypeFilter("datatype-a");
        withExpectedNextSeekCounts(2, 1);
        assertExpected("20250403", "datatype-a");

        // enabling time travel means we will return keys for the first and second
        withTimeTravel(true);
        withExpectedNextSeekCounts(4, 1);
        assertExpected("20250403", "ACTIVITY");
    }

    private void write(String shard, String type, String date, String datatype, String field, int... offsets) {
        Key key = new Key(shard, type, date + "\u0000" + datatype + "\u0000" + field);
        BitSet bitset = new BitSet();
        for (int offset : offsets) {
            bitset.set(offset);
        }
        Value value = new Value(bitset.toByteArray());
        data.put(key, value);
    }

    private void assertExpected(String date, String type) throws IOException {
        assertExpected(date, date, type);
    }

    private void assertExpected(String start, String stop, String type) throws IOException {
        Range range = createRangeForDate(start, stop);
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);

        Collection<ByteSequence> columnFamily = Collections.singleton(new ArrayByteSequence(type.getBytes()));

        StatEnabledDateIndexIterator iter = new StatEnabledDateIndexIterator();
        iter.init(source, options, env);
        iter.seek(range, columnFamily, true);

        while (iter.hasTop()) {
            Key key = iter.getTopKey();
            log.info("k: {}", key.toStringNoTime());
            iter.next();
        }

        assertEquals(expectedNextCount, iter.getNextCount(), "expected next count");
        assertEquals(expectedSeekCount, iter.getSeekCount(), "expected seek count");
    }

    private Range createRangeForDate(String start, String stop) {
        return new Range(start, stop + '~');
    }

    private void withDatatypeFilter(String... types) {
        options.put(DateIndexIterator.DATATYPE_FILTER, Joiner.on(',').join(types));
    }

    private void withStartEndDate(String startDate, String endDate) {
        options.put(DateIndexIterator.MINIMUM_DATE, startDate);
        options.put(DateIndexIterator.MAXIMUM_DATE, endDate);
    }

    private void withField(String field) {
        options.put(DateIndexIterator.FIELD, field);
    }

    private void withTimeTravel(boolean flag) {
        options.put(DateIndexIterator.TIME_TRAVEL_ENABLED, Boolean.toString(flag));
    }

    private void withExpectedNextSeekCounts(int expectedNextCount, int expectedSeekCount) {
        this.expectedNextCount = expectedNextCount;
        this.expectedSeekCount = expectedSeekCount;
    }

}
