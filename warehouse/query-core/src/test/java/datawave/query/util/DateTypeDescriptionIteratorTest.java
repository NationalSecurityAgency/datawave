package datawave.query.util;

import static datawave.query.util.DateIndexHelper.DateTypeDescription;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import datawave.query.iterator.SourceManagerTest;

public class DateTypeDescriptionIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(DateTypeDescriptionIteratorTest.class);

    private final SortedMap<Key,Value> data = new TreeMap<>();
    private final Map<String,String> options = new HashMap<>();

    private SourceManagerTest.MockIteratorEnvironment env;

    private int expectedNextCount = 0;
    private int expectedSeekCount = 0;
    private DateTypeDescription description;
    private final Set<String> expectedFields = new HashSet<>();
    private String expectedBeginDate;
    private String expectedEndDate;

    @BeforeEach
    public void setup() {
        // only clear the backing data between tests
        data.clear();
        // test state can be cleared multiple times per test
        clearState();
    }

    private void clearState() {
        env = new SourceManagerTest.MockIteratorEnvironment();
        options.clear();
        expectedNextCount = 0;
        expectedSeekCount = 0;
        description = null;
        expectedFields.clear();
        ;
        expectedBeginDate = null;
        expectedEndDate = null;
    }

    @Test
    public void testFullScan() throws IOException {
        write("20250404", "LOADED", "20250404", "datatype-a", "FIELD_A", 1);
        withExpectedNextSeekCounts(2, 1);
        withExpectedFields("FIELD_A");
        withExpectedBeginEndDates("20250404", "20250404");
        drive("20250404", "20250404", "LOADED");
    }

    @Test
    public void testScanFilteredByDataType() throws IOException {
        // datatype-a from 3rd to 4th
        write("20250404", "LOADED", "20250403", "datatype-a", "FIELD_A", 1);
        write("20250404", "LOADED", "20250404", "datatype-a", "FIELD_A", 1);
        // datatype-b from 2nd to 4th
        write("20250404", "LOADED", "20250402", "datatype-b", "FIELD_A", 1);
        write("20250404", "LOADED", "20250403", "datatype-b", "FIELD_A", 1);
        write("20250404", "LOADED", "20250404", "datatype-b", "FIELD_A", 1);

        withExpectedNextSeekCounts(2, 1);
        withExpectedFields("FIELD_A");
        withExpectedBeginEndDates("20250403", "20250404");
        withDatatypeFilter("datatype-a");
        drive("20250404", "20250404", "LOADED");

        withExpectedNextSeekCounts(2, 1);
        withExpectedFields("FIELD_A");
        withExpectedBeginEndDates("20250402", "20250404");
        withDatatypeFilter("datatype-b");
        drive("20250404", "20250404", "LOADED");
    }

    private void drive(String start, String stop, String type) throws IOException {
        Range range = createRangeForDate(start, stop);
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);

        Collection<ByteSequence> columnFamily = Collections.singleton(new ArrayByteSequence(type.getBytes()));

        StatDateTypeDescriptionIterator iter = new StatDateTypeDescriptionIterator();
        iter.init(source, options, env);
        iter.seek(range, columnFamily, true);

        int count = 0;
        while (iter.hasTop()) {
            count++;

            if (log.isTraceEnabled()) {
                log.trace("tk: {}", iter.getTopKey());
            }

            Value value = iter.getTopValue();
            description = DateTypeDescription.deserializeFromString(new String(value.get()));

            iter.next();
        }

        assertEquals(1, count);
        assertEquals(expectedNextCount, iter.getNextCount(), "expected next count");
        assertEquals(expectedSeekCount, iter.getSeekCount(), "expected seek count");

        assertNotNull(description);
        assertEquals(expectedBeginDate, description.getDateRange()[0]);
        assertEquals(expectedEndDate, description.getDateRange()[1]);
        assertEquals(expectedFields, description.getFields());

        // might want to drive an iterator multiple times
        clearState();
    }

    private Range createRangeForDate(String start, String stop) {
        return new Range(start, stop + '~');
    }

    private void withExpectedNextSeekCounts(int expectedNextCount, int expectedSeekCount) {
        this.expectedNextCount = expectedNextCount;
        this.expectedSeekCount = expectedSeekCount;
    }

    private void withExpectedFields(String... fields) {
        expectedFields.addAll(List.of(fields));
    }

    private void withExpectedBeginEndDates(String beginDate, String endDate) {
        this.expectedBeginDate = beginDate;
        this.expectedEndDate = endDate;
    }

    private void withDatatypeFilter(String... types) {
        options.put(DateIndexIterator.DATATYPE_FILTER, Joiner.on(',').join(types));
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

    private class StatDateTypeDescriptionIterator extends DateTypeDescriptionIterator {

        private int next = 0;
        private int seek = 0;

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            seek++;
            super.seek(range, columnFamilies, inclusive);
        }

        @Override
        public void next() throws IOException {
            next++;
            super.next();
        }

        public int getNextCount() {
            return next;
        }

        public int getSeekCount() {
            return seek;
        }
    }
}
