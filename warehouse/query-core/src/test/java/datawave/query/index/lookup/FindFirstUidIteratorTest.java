package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import datawave.ingest.protobuf.Uid;

public class FindFirstUidIteratorTest {

    private final Map<String,String> options = new HashMap<>();

    // assert variables
    private String expectedTopShard;
    private String expectedUid;

    @BeforeEach
    public void beforeEach() {
        options.clear();
        expectedTopShard = null;
        expectedUid = null;
    }

    @Test
    public void testSingleFieldHit_uids() {
        setOptionFields(Set.of("FIELD_A"));
        setExpectedShardUid("20240203_0", "u.i.d-1");
        drive();
    }

    @Test
    public void testSingleFieldHit_tldUids() {
        setOptionFields(Set.of("FIELD_B"));
        setExpectedShardUid("20240204_0", "u.i.d-1.1");
        drive();
    }

    @Test
    public void testSingleFieldHit_noUids() {
        setOptionFields(Set.of("FIELD_C"));
        setExpectedShard("20240203_0");
        drive();
    }

    @Test
    public void testSingleFieldMiss() {
        setOptionFields(Set.of("FIELD_Z"));
        drive();
    }

    @Test
    public void testMultiFieldAllFieldsHit() {
        setOptionFields(Set.of("FIELD_A", "FIELD_B"));
        setExpectedShardUid("20240203_0", "u.i.d-1");
        drive();
    }

    @Test
    public void testMultiFieldSomeFieldsHit() {
        setOptionFields(Set.of("FIELD_A", "FIELD_Z"));
        setExpectedShardUid("20240203_0", "u.i.d-1");
        drive();
    }

    @Test
    public void testMultiFieldAllFieldsMiss() {
        setOptionFields(Set.of("FIELD_X", "FIELD_Y", "FIELD_Z"));
        drive();
    }

    @Test
    public void testTldOption() {
        setOptionFields(Set.of("FIELD_B"));
        setOptionTLD(true);
        // dot notation which indicates a child object is dropped
        setExpectedShardUid("20240204_0", "u.i.d-1");
        drive();
    }

    @Test
    public void testCollapseOption_uids() {
        setOptionFields(Set.of("FIELD_A"));
        setOptionCollapse(true);
        // uids are dropped
        setExpectedShard("20240203_0");
        drive();
    }

    @Test
    public void testCollapseOption_tldUids() {
        setOptionFields(Set.of("FIELD_B"));
        setOptionCollapse(true);
        // uids are dropped
        setExpectedShard("20240204_0");
        drive();
    }

    @Test
    public void testCollapseOption_noUids() {
        setOptionFields(Set.of("FIELD_C"));
        setOptionCollapse(true);
        // data with no uids experiences no change
        setExpectedShard("20240203_0");
        drive();
    }

    @Test
    public void testTriggerSeekToNextFieldBasedOnEndDate() {
        setOptionFields(Set.of("FIELD_A", "FIELD_B"));
        setExpectedShardUid("20240204_0", "u.i.d-1");
        drive(createDataForSeekToNextField());
    }

    @Test
    public void testTriggerSeekToStartDate() {
        setOptionFields(Set.of("FIELD_A", "FIELD_B"));
        setExpectedShardUid("20240204_0", "u.i.d-1");
        drive(createDataForSeekToStartDate());
    }

    @Test
    public void testTriggerMultipleSeeks() {
        setOptionFields(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
        setExpectedShardUid("20240204_0", "u.i.d-1");
        drive(createDataForMultipleSeeks());
    }

    private void drive() {
        drive(createDefaultData());
    }

    private void drive(SortedMap<Key,Value> data) {
        assertNotNull(options);
        assertNotNull(data);

        try {
            FindFirstUidIterator iterator = iteratorFromOptions(options, data);

            assertEquals(expectedTopShard != null, iterator.hasTop());

            if (expectedTopShard != null) {
                Key topKey = iterator.getTopKey();
                IndexInfo info = infoFromValue(iterator.getTopValue());

                assertEquals(expectedTopShard, topKey.getColumnQualifier().toString());

                if (expectedUid == null) {
                    assertTrue(info.uids().isEmpty(), "expected 0 uids but found " + info.uids().size());
                } else {
                    // assert uids
                    assertEquals(1, info.uids().size());
                    String uid = info.uids().iterator().next().getUid();
                    // uid is the datatype + null byte + uid, so just do an endsWith
                    assertTrue(uid.endsWith(expectedUid), "uid: " + uid + " did not match expected: " + expectedUid);
                }

                iterator.next();
            }

            assertFalse(iterator.hasTop());

        } catch (Exception e) {
            fail("Failed with exception", e);
        }
    }

    private void setOptionCollapse(boolean collapse) {
        Preconditions.checkNotNull(options);
        options.put(FindFirstUidIterator.COLLAPSE_OPT, Boolean.toString(collapse));
    }

    private void setOptionTLD(boolean tld) {
        Preconditions.checkNotNull(options);
        options.put(FindFirstUidIterator.IS_TLD_OPT, Boolean.toString(tld));
    }

    private void setOptionFields(Set<String> fields) {
        Preconditions.checkNotNull(options);
        options.put(FindFirstUidIterator.FIELDS_OPT, Joiner.on(',').join(fields));
    }

    private void setExpectedShard(String shard) {
        this.expectedTopShard = shard;
    }

    private void setExpectedShardUid(String shard, String uid) {
        this.expectedTopShard = shard;
        this.expectedUid = uid;
    }

    private FindFirstUidIterator iteratorFromOptions(Map<String,String> options, SortedMap<Key,Value> data) throws IOException {
        FindFirstUidIterator iterator = new FindFirstUidIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        // this step might be unnecessary, or require updated args
        iterator.seek(rangeFromOptions(), columnFamiliesFromOptions(), false);
        return iterator;
    }

    private Range rangeFromOptions() {
        if (options.containsKey(FindFirstUidIterator.FIELDS_OPT)) {
            String opt = options.get(FindFirstUidIterator.FIELDS_OPT);
            TreeSet<String> fields = new TreeSet<>(Splitter.on(',').splitToList(opt));

            Key start = new Key("value", fields.first(), "20240203");
            Key stop = new Key("value", fields.last(), "20240204_\uffff");
            return new Range(start, true, stop, false);
        }
        return new Range();
    }

    private Collection<ByteSequence> columnFamiliesFromOptions() {
        if (options.containsKey(FindFirstUidIterator.FIELDS_OPT)) {
            String opt = options.get(FindFirstUidIterator.FIELDS_OPT);
            TreeSet<String> fields = new TreeSet<>(Splitter.on(',').splitToList(opt));

            Collection<ByteSequence> columnFamilies = new HashSet<>();
            for (String field : fields) {
                columnFamilies.add(new ArrayByteSequence(field));
            }
            return columnFamilies;
        }
        return Collections.emptySet();
    }

    private SortedMap<Key,Value> createDefaultData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // FIELD_A has multiple uids per 'unique' value
        data.put(new Key("value", "FIELD_A", "20240203_0\0datatype-a"), createValue("u.i.d-1", "u.i.d-2", "u.i.d-3"));
        data.put(new Key("value", "FIELD_A", "20240203_1\0datatype-a"), createValue("u.i.d-4", "u.i.d-5", "u.i.d-6"));
        data.put(new Key("value", "FIELD_A", "20240203_2\0datatype-a"), createValue("u.i.d-7", "u.i.d-8", "u.i.d-9"));
        // FIELD_B has some tld uids
        data.put(new Key("value", "FIELD_B", "20240204_0\0datatype-a"), createValue("u.i.d-1.1", "u.i.d-1.2", "u.i.d-1.3"));
        data.put(new Key("value", "FIELD_B", "20240204_1\0datatype-a"), createValue("u.i.d-2.1", "u.i.d-2.2", "u.i.d-2.3"));
        data.put(new Key("value", "FIELD_B", "20240204_2\0datatype-a"), createValue("u.i.d-3.1", "u.i.d-3.2", "u.i.d-3.3"));
        // FIELD_C has no uids
        data.put(new Key("value", "FIELD_C", "20240203_0\0datatype-a"), createValue(23));
        data.put(new Key("value", "FIELD_C", "20240203_1\0datatype-a"), createValue(55));
        data.put(new Key("value", "FIELD_C", "20240203_2\0datatype-a"), createValue(89));
        return data;
    }

    private SortedMap<Key,Value> createDataForSeekToNextField() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // FIELD_A only has data after the end date, thus triggering a seek to FIELD_B
        for (int i = 1; i < 15; i++) {
            data.put(new Key("value", "FIELD_A", "20240603_" + i + "\0datatype-a"), createValue("u.i.d-1"));
        }
        // FIELD_B has the data we care about
        data.put(new Key("value", "FIELD_B", "20240204_0\0datatype-a"), createValue("u.i.d-1"));
        return data;
    }

    private SortedMap<Key,Value> createDataForSeekToStartDate() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // FIELD_A only has data after the end date, thus triggering a seek to FIELD_B
        for (int i = 1; i < 15; i++) {
            data.put(new Key("value", "FIELD_A", "20240603_" + i + "\0datatype-a"), createValue("u.i.d-1"));
        }
        // the key we care about
        data.put(new Key("value", "FIELD_B", "20240204_0\0datatype-a"), createValue("u.i.d-1"));
        return data;
    }

    private SortedMap<Key,Value> createDataForMultipleSeeks() {
        SortedMap<Key,Value> data = new TreeMap<>();
        // FIELD_A only has data after the end date, thus triggering a seek to FIELD_B
        for (int i = 1; i < 15; i++) {
            data.put(new Key("value", "FIELD_A", "20240603_" + i + "\0datatype-a"), createValue("u.i.d-1"));
        }

        // FIELD_B only has data BEFORE the start date, but the seek range includes the start date, so this data is skipped
        for (int i = 1; i < 15; i++) {
            data.put(new Key("value", "FIELD_B", "20240103_" + i + "\0datatype-a"), createValue("u.i.d-1"));
        }

        // FIELD_C has the data we care about
        data.put(new Key("value", "FIELD_C", "20240204_0\0datatype-a"), createValue("u.i.d-1"));
        return data;
    }

    private Value createValue(String... uids) {
        Preconditions.checkState(uids.length < 20);
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(uids.length);
        builder.addAllUID(List.of(uids));
        builder.setIGNORE(false);
        return new Value(builder.build().toByteArray());
    }

    private Value createValue(int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(count);
        builder.setIGNORE(true);
        return new Value(builder.build().toByteArray());
    }

    private IndexInfo infoFromValue(Value value) throws IOException {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(value.get())));
        return indexInfo;
    }
}
