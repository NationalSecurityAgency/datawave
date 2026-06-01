package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;

/**
 * Note: because this class doesn't use combiners each key must be distinct
 */
public class TruncatedIndexIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexIteratorTest.class);

    private final String DEFAULT_DATE = "20251010";
    private final String DEFAULT_DATATYPE = "datatype-a";
    private final String DEFAULT_VISIBILITY = "VIZ-A";

    private final List<Entry<Key,Value>> expected = new ArrayList<>();
    private final List<Entry<Key,Value>> results = new ArrayList<>();
    private final SortedMap<Key,Value> data = new TreeMap<>();

    private String field;
    private String value;
    private String startDate;
    private String endDate;

    @BeforeEach
    public void beforeEach() {
        expected.clear();
        results.clear();
        data.clear();

        field = null;
        value = null;
        startDate = null;
        endDate = null;
    }

    @Test
    public void testSingleKeySingleBit() throws IOException {
        write(createKey("value-a", "FIELD_A"), createValue(0));
        expect(createKey("value-a", "FIELD_A"), createValue(0));
        withFieldValue("FIELD_A", "value-a");
        withDateRange("20251010", "20251010");
        iterate();
    }

    @Test
    public void testSingleKeyMultiBit() throws IOException {
        write(createKey("value-a", "FIELD_A"), createValue(0, 17));
        expect(createKey("value-a", "FIELD_A"), createValue(0, 17));
        withFieldValue("FIELD_A", "value-a");
        withDateRange("20251010", "20251010");
        iterate();
    }

    @Test
    public void testAggregateDatatypes() throws IOException {
        write(createKey("value-a", "FIELD_A", DEFAULT_DATE, "datatype-a"), createValue(0));
        write(createKey("value-a", "FIELD_A", DEFAULT_DATE, "datatype-b"), createValue(17));
        expect(createKey("value-a", "FIELD_A"), createValue(0, 17));
        withFieldValue("FIELD_A", "value-a");
        withDateRange("20251010", "20251010");
        iterate();
    }

    @Test
    public void testAggregateVisibilities() throws IOException {
        write(createKey("value-a", "FIELD_A", DEFAULT_DATE, DEFAULT_DATATYPE, "VIZ-A"), createValue(0));
        write(createKey("value-a", "FIELD_A", DEFAULT_DATE, DEFAULT_DATATYPE, "VIZ-B"), createValue(17));
        expect(createKey("value-a", "FIELD_A"), createValue(0, 17));
        withFieldValue("FIELD_A", "value-a");
        withDateRange("20251010", "20251010");
        iterate();
    }

    // each call to this iterator should be restricted to a single day, but maybe not
    @Test
    public void testMultipleDays() throws IOException {
        write(createKey("value-a", "FIELD_A", "20251010"), createValue(0));
        write(createKey("value-a", "FIELD_A", "20251011"), createValue(1));
        write(createKey("value-a", "FIELD_A", "20251012"), createValue(2));
        expect(createKey("value-a", "FIELD_A", "20251010"), createValue(0));
        expect(createKey("value-a", "FIELD_A", "20251011"), createValue(1));
        expect(createKey("value-a", "FIELD_A", "20251012"), createValue(2));
        withFieldValue("FIELD_A", "value-a");
        withDateRange("20251010", "20251012");
        iterate();
    }

    private void iterate() throws IOException {
        Key start = new Key(value, field, startDate);
        Key end = new Key(value, field, endDate + "\u0000\uFFFF");
        Range range = new Range(start, end);

        Collection<ByteSequence> cfs = Set.of(new ArrayByteSequence(field));

        TruncatedIndexIterator iter = new TruncatedIndexIterator();
        iter.init(new SortedMapIterator(data), Collections.emptyMap(), new MockIteratorEnvironment());
        iter.seek(range, cfs, true);

        while (iter.hasTop()) {
            Key key = iter.getTopKey();
            Value value = iter.getTopValue();
            results.add(new SimpleEntry<>(key, value));
            iter.next();
        }

        if (expected.size() != results.size()) {
            log.warn("expected {} results but got {}", expected.size(), results.size());
        }
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getKey(), results.get(i).getKey());
            BitSet expectedBitSet = BitSet.valueOf(expected.get(i).getValue().get());
            BitSet resultBitSet = BitSet.valueOf(results.get(i).getValue().get());
            assertEquals(expectedBitSet, resultBitSet);
        }
        assertEquals(expected.size(), results.size());
    }

    private void withFieldValue(String field, String value) {
        this.field = field;
        this.value = value;
    }

    private void withDateRange(String start, String end) {
        this.startDate = start;
        this.endDate = end;
    }

    private void write(Key key, Value value) {
        data.put(key, value);
    }

    private void expect(Key key, Value value) {
        expected.add(new SimpleEntry<>(key, value));
    }

    private Key createKey(String value, String field) {
        return createKey(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, DEFAULT_VISIBILITY);
    }

    private Key createKey(String value, String field, String date) {
        return createKey(value, field, date, DEFAULT_DATATYPE, DEFAULT_VISIBILITY);
    }

    private Key createKey(String value, String field, String date, String datatype) {
        return createKey(value, field, date, datatype, DEFAULT_VISIBILITY);
    }

    private Key createKey(String value, String field, String date, String datatype, String visibility) {
        return new Key(value, field, date + "\u0000" + datatype, visibility);
    }

    private Value createValue(int... offsets) {
        BitSet bits = createBitSet(offsets);
        return new Value(bits.toByteArray());
    }

    private BitSet createBitSet(int... offsets) {
        BitSet bitSet = new BitSet();
        for (int offset : offsets) {
            bitSet.set(offset);
        }
        return bitSet;
    }

}
