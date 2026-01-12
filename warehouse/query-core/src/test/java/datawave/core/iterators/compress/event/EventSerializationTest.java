package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.iterator;
import static datawave.core.iterators.compress.CompressionTestUtil.list;
import static datawave.core.iterators.compress.CompressionTestUtil.shuffle;
import static datawave.core.iterators.compress.CompressionTestUtil.toSortedMap;
import static datawave.core.iterators.compress.CompressionTestUtil.uid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.iterators.compress.KeyGroup;
import datawave.data.hash.UID;
import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;

public class EventSerializationTest implements EventSerializationTestCases {

    private static final Logger log = LoggerFactory.getLogger(EventSerializationTest.class);

    private final String row = "20251010_123";
    private final String cf = "datatype-a\0" + uid(1);
    private final String cq = "FIELD_A\0value-a";
    private final String viz = "VIZ-A";
    private final long ts = Instant.parse("2011-12-03T10:15:30Z").toEpochMilli();
    private final String uid = UID.builder().newId("abc.def.ghi".getBytes(), (Date) null).toString();

    // serializer iterator options
    private final Map<String,String> options = new HashMap<>();

    @BeforeEach
    public void beforeEach() {
        options.clear();
        options.put(EventSerializationIterator.VERSION_OPT, "1");
        options.put(EventSerializationIterator.THRESHOLD_OPT, "512");
        options.put(EventSerializationIterator.ALGORITHM_OPT, EventSerializationUtil.GZIP);
    }

    @Test
    @Override
    public void testOneOfEachKeyType() throws IOException {
        Key fiKey = new Key(row, "fi\0FIELD_A", "value-a\0datatype\0" + uid, viz, ts);
        Key eventKey = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", viz, ts);
        Key tfKey = new Key(row, "tf", "datatype\0" + uid + "value-a\0FIELD_A", viz, ts);

        List<Key> inputs = list(fiKey, eventKey, tfKey);
        Key compressedEventKey = new Key(row, "datatype\0" + uid, "raw\u00001-1", viz, ts);
        List<Key> compressed = list(fiKey, compressedEventKey, tfKey);
        assertFullIteration(inputs, compressed);
    }

    @Test
    @Override
    public void testDifferentVisibilities() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-B", ts);
        Key k3 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-C", ts);
        Key c1 = EventMarkerUtil.createMarker(k1, "1", 1);
        Key c2 = EventMarkerUtil.createMarker(k2, "1", 1);
        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        assertFullIteration(list(k1, k2, k3), list(c1, c2, c3));
    }

    @Test
    @Override
    public void testDifferentTimestamps() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, 10);
        Key k2 = new Key(row, cf, "FIELD_A\0value-a", viz, 11);
        Key k3 = new Key(row, cf, "FIELD_A\0value-a", viz, 12);
        Key c1 = EventMarkerUtil.createMarker(k1, "1", 1);
        Key c2 = EventMarkerUtil.createMarker(k2, "1", 1);
        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        // no versioning iterator so all keys are retained
        assertFullIteration(list(k1, k2, k3), list(c1, c2, c3));
    }

    @Test
    @Override
    public void testDifferentDeleteFlags() throws IOException {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, false);
        Key d1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, true);

        byte[] compressedCqBytes = "raw\u00001-1".getBytes();
        Key c1 = new Key(row.getBytes(), cf.getBytes(), compressedCqBytes, viz.getBytes(), ts, true);
        Key c2 = new Key(row.getBytes(), cf.getBytes(), compressedCqBytes, viz.getBytes(), ts, false);
        assertFullIteration(list(k1, d1), list(c1, c2));
    }

    @Test
    @Override
    public void testEvent() throws IOException {
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> values = List.of("value-a", "value-b", "value-c");

        SortedSet<Key> event = new TreeSet<>();
        for (String field : fields) {
            for (String value : values) {
                String cq = field + "\0" + value;
                Key key = new Key(row, cf, cq, viz, ts);
                event.add(key);
            }
        }

        Key compressed = new Key(row, cf, "raw\u00001-9", viz, ts);
        assertFullIteration(list(event), List.of(compressed));
    }

    @Test
    @Override
    public void testEventWithGroupingContext() throws IOException {
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> values = List.of("value-a", "value-b");
        List<String> groups = List.of(".1.1", ".1.2");

        SortedSet<Key> event = new TreeSet<>();
        for (String field : fields) {
            for (String value : values) {
                for (String group : groups) {
                    String cq = field + "\0" + value + group;
                    Key key = new Key(row, cf, cq, viz, ts);
                    event.add(key);
                }
            }
        }

        Key compressed = new Key(row, cf, "raw\u00001-12", viz, ts);
        assertFullIteration(list(event), List.of(compressed));
    }

    @Test
    @Override
    public void testTldEvent() throws IOException {
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> values = List.of("value-a", "value-b", "value-c");
        List<String> children = List.of(".1", ".2", ".3");

        SortedSet<Key> event = new TreeSet<>();
        for (String field : fields) {
            for (String value : values) {
                String cq = field + "\0" + value;
                Key key = new Key(row, cf, cq, viz, ts);
                event.add(key);

                // add keys for children
                for (String child : children) {
                    Key childKey = new Key(row, cf + child, cq, viz, ts);
                    event.add(childKey);
                }
            }
        }

        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-9", viz, ts));
        }
        assertFullIteration(list(event), list(compressed));
    }

    @Test
    @Override
    public void testTldEventWithGroupingContext() throws IOException {
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> values = List.of("value-a", "value-b", "value-c");
        List<String> children = List.of(".1", ".2", ".3");
        List<String> groups = List.of(".1.1", ".1.2");

        SortedSet<Key> event = new TreeSet<>();
        for (String field : fields) {
            for (String value : values) {
                String cq = field + "\0" + value;
                Key key = new Key(row, cf, cq, viz, ts);
                event.add(key);

                // add keys for children, only children get grouping context
                for (String child : children) {
                    for (String group : groups) {
                        Key childKey = new Key(row, cf + child, cq + group, viz, ts);
                        event.add(childKey);
                    }
                }
            }
        }

        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-18", viz, ts));
        }
        assertFullIteration(list(event), list(compressed));
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedData() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);

        SortedMap<Key,Value> inputs = new TreeMap<>();
        inputs.putAll(toSortedMap(List.of(k1, k2)));
        // add k3 as the latent data
        compress(k3).forEach(pair -> inputs.put(pair.getKey(), pair.getValue()));

        List<Key> expected = List.of(k1, k2, k3);
        List<Key> compressed = List.of(new Key(row, cf, "raw\u00001-3", viz, ts));
        assertFullIteration(inputs, expected, compressed);
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedData() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);

        SortedMap<Key,Value> inputs = new TreeMap<>();
        compress(List.of(k1, k2)).forEach(pair -> inputs.put(pair.getKey(), pair.getValue()));
        // add k3 as the latent data
        inputs.put(k3, new Value());

        List<Key> expected = List.of(k1, k2, k3);
        List<Key> compressed = List.of(new Key(row, cf, "raw\u00001-3", viz, ts));
        assertFullIteration(inputs, expected, compressed);
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedDataWithSamePartitionId() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);

        SortedMap<Key,Value> inputs = new TreeMap<>();
        inputs.put(k1, new Value());
        compress(k2).forEach(pair -> inputs.put(pair.getKey(), pair.getValue()));

        List<Key> expected = List.of(k1, k2);
        List<Key> compressed = List.of(new Key(row, cf, "raw\u00001-2", viz, ts));
        assertFullIteration(inputs, expected, compressed);
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedDataWithSamePartitionId() throws IOException {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);

        SortedMap<Key,Value> inputs = new TreeMap<>();
        compress(k1).forEach(pair -> inputs.put(pair.getKey(), pair.getValue()));
        // k2 is latent data
        inputs.put(k2, new Value());

        List<Key> expected = List.of(k1, k2);
        List<Key> compressed = List.of(new Key(row, cf, "raw\u00001-2", viz, ts));
        assertFullIteration(inputs, expected, compressed);
    }

    @Test
    @Override
    public void testLargeEventTriggersCompressionOfSerializedData() throws IOException {
        List<Key> inputs = getLargeEvent();
        List<Key> compressed = List.of(new Key(row, cf, "gzip\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), compressed);
    }

    @Test
    @Override
    public void testLargeEventCompressedAfterLoweringTheCompressionThreshold() throws IOException {
        List<Key> inputs = getLargeEvent();

        options.put(EventSerializationIterator.THRESHOLD_OPT, "1024");
        List<Key> raw = List.of(new Key(row, cf, "raw\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), raw);

        List<Key> compressed = List.of(new Key(row, cf, "gzip\u00001-45", viz, ts));
        options.put(EventSerializationIterator.THRESHOLD_OPT, "512");
        assertCompression(toSortedMap(inputs), compressed);
    }

    @Test
    @Override
    public void testLargeEventUncompressedAfterIncreasingTheCompressionThreshold() throws IOException {
        List<Key> inputs = getLargeEvent();

        // default threshold of 512 triggers compression
        List<Key> compressed = List.of(new Key(row, cf, "gzip\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), compressed);

        // running against with a higher threshold should return an uncompressed document
        options.put(EventSerializationIterator.THRESHOLD_OPT, "1024");
        List<Key> raw = List.of(new Key(row, cf, "raw\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), raw);
    }

    @Test
    @Override
    public void testLargeEventCompressionAlgorithmChanges() throws IOException {
        List<Key> inputs = getLargeEvent();

        // default threshold of 512 triggers compression
        List<Key> gzip = List.of(new Key(row, cf, "gzip\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), gzip);

        // changed algorithm changes the compression key
        options.put(EventSerializationIterator.ALGORITHM_OPT, "zstd");
        List<Key> zstd = List.of(new Key(row, cf, "zstd\u00001-45", viz, ts));
        assertCompression(toSortedMap(inputs), zstd);
    }

    protected List<Key> getLargeEvent() {
        List<Key> inputs = new ArrayList<>();
        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E")) {
            for (int i = 0; i < 9; i++) {
                inputs.add(new Key(row, cf, field + "\0value-" + i, viz, ts));
            }
        }
        return inputs;
    }

    /**
     * Drives two distinct cases given input keys and expected compressed keys.
     * <p>
     * First perform a round trip iteration via {@link #assertRoundTrip(SortedMap, List)}.
     * <p>
     * Then perform a compression iteration via {@link #assertCompression(SortedMap, List)}
     *
     * @param inputs
     *            the input keys
     * @param compressed
     *            the expected compressed keys
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertFullIteration(List<Key> inputs, List<Key> compressed) throws IOException {
        assertFullIteration(toSortedMap(inputs), new ArrayList<>(inputs), compressed);
    }

    /**
     * Performs a battery of tests using the provided inputs
     * <ul>
     * <li>assert full round trip via {@link #assertRoundTrip(SortedMap, List)}</li>
     * <li>assert compressed keys via {@link #assertCompression(SortedMap, List)}</li>
     * <li>assert direct seeks via {@link #assertRandomDirectSeeks(SortedMap, List)}</li>
     * <li>assert following seeks via {@link #assertRandomFollowingSeeks(SortedMap, List)}</li>
     * </ul>
     *
     * @param inputs
     *            the input keys
     * @param compressed
     *            the expected compressed keys
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertFullIteration(SortedMap<Key,Value> inputs, List<Key> expected, List<Key> compressed) throws IOException {
        assertRoundTrip(inputs, expected);
        assertCompression(inputs, compressed);
        assertRandomDirectSeeks(inputs, expected);
        assertRandomFollowingSeeks(inputs, expected);
    }

    /**
     * Verify the input keys persist after a round trip serialization and deserialization
     *
     * @param inputs
     *            the input keys, also the expected keys
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertRoundTrip(SortedMap<Key,Value> inputs, List<Key> expected) throws IOException {
        SortedKeyValueIterator<Key,Value> source = iterator(inputs);
        EventSerializationIterator serializer = serializer(source);
        EventDeserializationIterator deserializer = deserializer(serializer);
        assertIterator(deserializer, expected);
    }

    /**
     * Verify the intermediate compressed keys via the serialization iterator
     *
     * @param inputs
     *            the input keys
     * @param compressed
     *            the expected compressed keys
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertCompression(SortedMap<Key,Value> inputs, List<Key> compressed) throws IOException {
        SortedKeyValueIterator<Key,Value> source = iterator(inputs);
        EventSerializationIterator serializer = serializer(source);
        assertIterator(serializer, compressed);
    }

    /**
     * Drive the iterator and assert results against the list of expected keys
     *
     * @param source
     *            the source iterator
     * @param expected
     *            the list of expected keys
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertIterator(SortedKeyValueIterator<Key,Value> source, List<Key> expected) throws IOException {
        Iterator<Key> expectedIter = expected.iterator();
        while (source.hasTop()) {
            Key tk = source.getTopKey();
            assertTrue(expectedIter.hasNext());
            assertEquals(expectedIter.next(), tk);
            source.next();
        }
        assertFalse(source.hasTop());
        assertFalse(expectedIter.hasNext());
    }

    /**
     * Verify seeking to each key within the event. The list of expected keys drives the seeks because the input could be a mix of compressed and uncompressed
     * keys and there will never be a seek issued that contains a compressed marker.
     *
     * @param inputs
     *            the input key values
     * @param expected
     *            the expected keys, used to drive the seeks
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertRandomDirectSeeks(SortedMap<Key,Value> inputs, List<Key> expected) throws IOException {
        SortedKeyValueIterator<Key,Value> source = iterator(inputs);
        EventSerializationIterator serializer = serializer(source);
        EventDeserializationIterator deserializer = deserializer(serializer);
        List<Key> shuffled = shuffle(expected);
        for (Key key : shuffled) {
            Range range = new Range(key, true, null, false);
            if (log.isDebugEnabled()) {
                log.debug("seek to: {}", key.toString());
            }
            deserializer.seek(range, Collections.emptySet(), false);
            Key top = deserializer.getTopKey();
            assertEquals(key, top);
        }
    }

    /**
     * Verify seeking past each key functions as expected. The list of expected keys drives the seeks because the input could be a mix of compressed and
     * uncompressed keys and there will never be a seek issued that contains a compressed marker.
     *
     * @param inputs
     *            the input key values
     * @param expected
     *            the expected keys, used to drive the seeks
     * @throws IOException
     *             if something goes wrong
     */
    protected void assertRandomFollowingSeeks(SortedMap<Key,Value> inputs, List<Key> expected) throws IOException {
        SortedKeyValueIterator<Key,Value> source = iterator(inputs);
        EventSerializationIterator serializer = serializer(source);
        EventDeserializationIterator deserializer = deserializer(serializer);
        TreeSet<Key> sortedKeys = new TreeSet<>(expected);
        List<Key> shuffled = shuffle(expected);
        for (Key key : shuffled) {
            Key next = key.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            Range range = new Range(next, false, null, false);
            deserializer.seek(range, Collections.emptySet(), false);
            Key top = deserializer.getTopKey();

            Key following = sortedKeys.higher(next);
            if (following == null) {
                assertNull(top);
            } else {
                assertEquals(following, top);
            }
        }
    }

    public EventSerializationIterator serializer(SortedKeyValueIterator<Key,Value> source) throws IOException {
        EventSerializationIterator iter = new EventSerializationIterator();
        iter.init(source, options, new MockIteratorEnvironment());
        iter.seek(new Range(), Collections.emptySet(), false);
        return iter;
    }

    public EventDeserializationIterator deserializer(SortedKeyValueIterator<Key,Value> source) throws IOException {
        EventDeserializationIterator iter = new EventDeserializationIterator();
        iter.init(source, Collections.emptyMap(), new MockIteratorEnvironment());
        iter.seek(new Range(), Collections.emptySet(), false);
        return iter;
    }

    protected List<Pair<Key,Value>> compress(Key key) throws IOException {
        return compress(List.of(key));
    }

    protected List<Pair<Key,Value>> compress(List<Key> keys) throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (Key key : keys) {
            data.put(key, new Value());
        }

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        source.seek(new Range(), Collections.emptySet(), false);

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(source);
        return compressed.getKeyValues();
    }
}
