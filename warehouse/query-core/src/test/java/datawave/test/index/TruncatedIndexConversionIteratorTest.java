package datawave.test.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.aggregator.TruncatedIndexConversionIterator;
import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;

class TruncatedIndexConversionIteratorTest implements IndexConversionTests {

    private final String DEFAULT_VISIBILITY = "VIZ-A";
    private final TreeMap<Key,Value> data = new TreeMap<>();
    private final List<Entry<Key,Value>> expected = new ArrayList<>();

    @BeforeEach
    public void beforeEach() {
        data.clear();
        expected.clear();
    }

    @Test
    public void testDuplicateKeysCollapse() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableValues() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-b", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-c", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-b", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-c", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableFields() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_B", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_C", "20250606_1\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-a", "FIELD_B", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-a", "FIELD_C", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableDays() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250607_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250608_1\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250607\0datatype-a"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250608\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableShards() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_2\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_3\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1, 2, 3));
        assertResults();
    }

    @Test
    public void testVariableDatatypes() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-b"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-c"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-b"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-c"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testInvertedDatatypes() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-c"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_2\0datatype-b"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_3\0datatype-a"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(3));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-b"), bitSetValue(2));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-c"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableVisibilities() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-A"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-B"), uidValue("uid-b"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-C"), uidValue("uid-c"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-A"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-B"), bitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-C"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testInvertedVisibilities() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-C"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_2\0datatype-a", "VIZ-B"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_3\0datatype-a", "VIZ-A"), uidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-A"), bitSetValue(3));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-B"), bitSetValue(2));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-C"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testVariableUids() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-b"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-c"));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testPermutationsAndCompareTables() {
        List<String> values = List.of("value-a", "value-b", "value-c");
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> dates = List.of("20250606", "20250607", "20250608");
        List<String> shards = List.of("_1", "_2", "_3");
        List<String> datatypes = List.of("datatype-a", "datatype-b", "datatype-c");
        List<String> visibilities = List.of("VIZ-A", "VIZ-B", "VIZ-C");
        List<String> uids = List.of("uid-a", "uid-b", "uid-c");
        for (String value : values) {
            for (String field : fields) {
                for (String date : dates) {
                    for (String num : shards) {
                        for (String datatype : datatypes) {
                            for (String visibility : visibilities) {
                                for (String uid : uids) {
                                    Key key = create(value, field, date + num + "\0" + datatype, visibility);
                                    Value v = uidValue(uid);
                                    write(key, v);
                                }
                            }
                        }
                    }
                }
            }
        }

        assertEquals(729, data.size());
        assertCount(243);
    }

    @Test
    public void testMixOfKeyStructures() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    @Test
    public void testDeletes() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), uidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), bitSetValue(1));
        assertResults();
    }

    private Key create(String row, String cf, String cq) {
        return create(row, cf, cq, DEFAULT_VISIBILITY);
    }

    private Key create(String row, String cf, String cq, String cv) {
        return new Key(row, cf, cq, cv);
    }

    private Value uidValue(String uid) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllUID(List.of(uid));
        b.setCOUNT(1);
        return new Value(b.build().toByteArray());
    }

    private Value bitSetValue(int... bits) {
        BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return new Value(bitSet.toByteArray());
    }

    private void write(Key k, Value v) {
        data.put(k, v);
    }

    public void expect(Key k, Value v) {
        expected.add(new SimpleEntry<>(k, v));
    }

    private void assertResults() {
        try {
            TruncatedIndexConversionIterator iter = new TruncatedIndexConversionIterator();
            iter.init(new SortedMapIterator(data), Collections.emptyMap(), new MockIteratorEnvironment());
            iter.seek(new Range(), Collections.emptySet(), true);

            List<Entry<Key,Value>> results = new ArrayList<>();

            while (iter.hasTop()) {
                Key k = iter.getTopKey();
                Value v = iter.getTopValue();
                results.add(new SimpleEntry<>(k, v));
                iter.next();
            }

            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).getKey(), results.get(i).getKey(), "key mismatch");

                BitSet expectedBits = BitSet.valueOf(expected.get(i).getValue().get());
                BitSet resultBits = BitSet.valueOf(results.get(i).getValue().get());
                assertEquals(expectedBits, resultBits, "bitset mismatch");
            }
        } catch (Exception e) {
            fail("Test failure", e);
        }
    }

    private void assertCount(int count) {
        try {
            TruncatedIndexConversionIterator iter = new TruncatedIndexConversionIterator();
            iter.init(new SortedMapIterator(data), Collections.emptyMap(), new MockIteratorEnvironment());
            iter.seek(new Range(), Collections.emptySet(), true);

            List<Entry<Key,Value>> results = new ArrayList<>();

            while (iter.hasTop()) {
                Key k = iter.getTopKey();
                Value v = iter.getTopValue();
                results.add(new SimpleEntry<>(k, v));
                iter.next();
            }

            assertEquals(count, results.size());
        } catch (Exception e) {
            fail("Test failure", e);
        }
    }
}
