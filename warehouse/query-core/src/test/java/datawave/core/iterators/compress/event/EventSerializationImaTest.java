package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.list;
import static datawave.core.iterators.compress.CompressionTestUtil.shuffle;
import static datawave.core.iterators.compress.CompressionTestUtil.uid;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addDeserializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addSerializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.removeDeserializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.removeSerializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.serialize;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setCompressionAlgorithm;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setCompressionThreshold;
import static datawave.query.RebuildingScannerTestHelper.INTERRUPT;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN;
import static datawave.query.RebuildingScannerTestHelper.getClient;
import static datawave.test.MacTestUtil.createOrRecreate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.hash.UID;
import datawave.util.TableName;

/**
 * Exercises the {@link EventSerializationIterator} in a variety of cases.
 * <p>
 * Because this test relies on {@link InMemoryAccumulo} the data will not change once written. For a test that alters the underlying data via compactions see
 * {@link EventSerializationImaTest}.
 */
public class EventSerializationImaTest implements EventSerializationTestCases {

    private final Logger log = LoggerFactory.getLogger(EventSerializationImaTest.class);

    private static final Value EMPTY_VALUE = new Value();
    private static final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");

    private final String row = "20251010_123";
    private final String cf = "datatype-a\0" + uid(1);
    private final String cq = "FIELD_A\0value-a";
    private final String viz = "VIZ-A";
    private final long ts = Instant.parse("2011-12-03T10:15:30Z").toEpochMilli();
    private final String uid = UID.builder().newId("abc.def.ghi".getBytes(), (Date) null).toString();

    private final Key fiKey = new Key(row, "fi\0FIELD_A", "value-a\0datatype\0" + uid, "VIZ-A", ts);
    private final Key eventKey = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
    private final Key tfKey = new Key(row, "tf", "datatype\0" + uid + "value-a\0FIELD_A", "VIZ-A", ts);

    private final String tableName = TableName.SHARD;

    private final InMemoryInstance instance = new InMemoryInstance(getClass().getName());
    private AccumuloClient client;
    private TableOperations tops;

    @BeforeEach
    public void beforeEach() throws AccumuloSecurityException {
        client = new InMemoryAccumuloClient("root", instance);
        tops = client.tableOperations();
        createOrRecreate(tops, tableName);
        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);
    }

    @AfterEach
    public void afterEach() {
        if (client != null) {
            client.close();
        }
    }

    private void write(Key... keys) {
        for (Key key : keys) {
            write(key, EMPTY_VALUE);
        }
    }

    private void write(Key key) {
        write(key, EMPTY_VALUE);
    }

    private void write(Key key, Value value) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail("Failed to write key: " + key, e);
            throw new RuntimeException(e);
        }
    }

    private void delete(Key... keys) {
        for (Key key : keys) {
            delete(key);
        }
    }

    private void delete(Key key) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(key.getRow());
            m.putDelete(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp());
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail("Failed to write key: " + key, e);
            throw new RuntimeException(e);
        }
    }

    private SortedMap<Key,Value> scan() {
        return scan(auths);
    }

    private SortedMap<Key,Value> scan(Authorizations authorizations) {
        AccumuloClient client = getRebuildingClient();
        try (Scanner scanner = client.createScanner(tableName, authorizations)) {
            scanner.setRange(new Range(row));

            SortedMap<Key,Value> results = new TreeMap<>();
            for (Map.Entry<Key,Value> entry : scanner) {
                log.trace("key: {}", entry.getKey());
                results.put(entry.getKey(), entry.getValue());
            }
            return results;
        } catch (TableNotFoundException e) {
            fail("Failed to scan table", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Seek to the provided range with default authorizations and return the first key
     *
     * @param range
     *            the range
     * @return the first key or null if no key exists
     */
    private Key seek(Range range) {
        return seek(range, auths);
    }

    /**
     * Seek to the provided range and return the first key
     *
     * @param range
     *            the range
     * @param authorizations
     *            the authorizations
     * @return the first key or null if no key exists
     */
    private Key seek(Range range, Authorizations authorizations) {
        AccumuloClient client = getRebuildingClient();
        try (Scanner scanner = client.createScanner(tableName, authorizations)) {
            scanner.setRange(range);
            for (Map.Entry<Key,Value> entry : scanner) {
                log.trace("key: {}", entry.getKey());
                return entry.getKey();
            }
            return null;
        } catch (TableNotFoundException e) {
            fail("Failed to scan table", e);
            throw new RuntimeException(e);
        }
    }

    private AccumuloClient getRebuildingClient() {
        try {
            return getClient(instance, "root", new PasswordToken(""), TEARDOWN.RANDOM, INTERRUPT.RANDOM);
        } catch (AccumuloSecurityException e) {
            fail("Failed to get rebuilding client", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    @Override
    public void testOneOfEachKeyType() {
        write(fiKey, eventKey, tfKey);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(fiKey, eventKey, tfKey));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = EventMarkerUtil.createMarker(eventKey, "1", 1);
        assertScanResults(results, list(fiKey, compressed, tfKey));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(fiKey, eventKey, tfKey));
    }

    @Test
    @Override
    public void testDifferentVisibilities() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-B", ts);
        Key k3 = new Key(row, cf, "FIELD_A\0value-a", "VIZ-C", ts);
        write(k1, k2, k3);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = EventMarkerUtil.createMarker(k1, "1", 1);
        Key c2 = EventMarkerUtil.createMarker(k2, "1", 1);
        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        assertScanResults(results, list(c1, c2, c3));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(k1, k2, k3));
    }

    /**
     * The iterators correctly produce keys per distinct timestamp, versioning iterator keeps the latest
     */
    @Test
    @Override
    public void testDifferentTimestamps() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, 10);
        Key k2 = new Key(row, cf, "FIELD_A\0value-a", viz, 11);
        Key k3 = new Key(row, cf, "FIELD_A\0value-a", viz, 12);

        write(k1, k2, k3);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k3));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        assertScanResults(results, list(c3));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(k3));
    }

    @Test
    @Override
    public void testDifferentDeleteFlags() {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, false);
        Key d1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, true);
        write(k1);
        delete(d1);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, Collections.emptyList());

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, Collections.emptyList());

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, Collections.emptyList());
    }

    @Test
    @Override
    public void testEvent() {
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
        event.forEach(this::write);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = new Key(row, cf, "raw\u00001-9", viz, ts);
        assertScanResults(results, List.of(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(event));
    }

    @Test
    @Override
    public void testEventWithGroupingContext() {
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
        event.forEach(this::write);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = new Key(row, cf, "raw\u00001-12", viz, ts);
        assertScanResults(results, List.of(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(event));
    }

    @Test
    @Override
    public void testTldEvent() {
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
        event.forEach(this::write);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-9", viz, ts));
        }
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(event));
    }

    @Test
    @Override
    public void testTldEventWithGroupingContext() {
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
        event.forEach(this::write);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-18", viz, ts));
        }
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, list(event));
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedData() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);

        write(k1, k2);
        EventCompressionTestUtil.serialize(k3, 1).forEach(pair -> write(pair.getKey(), pair.getValue()));

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = new Key(row, cf, "raw\u00001-3", viz, ts);
        SortedSet<Key> compressed = new TreeSet<>(List.of(c1));
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        Key c0 = new Key(row, cf, "raw\u00001-1", viz, ts);
        assertScanResults(results, list(k1, k2, c0));
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedData() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);

        serialize(List.of(k1, k2)).forEach(pair -> write(pair.getKey(), pair.getValue()));
        write(k3);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = new Key(row, cf, "raw\u00001-3", viz, ts);
        SortedSet<Key> compressed = new TreeSet<>(List.of(c1));
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        Key c0 = new Key(row, cf, "raw\u00001-2", viz, ts);
        assertScanResults(results, list(k3, c0));
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedDataWithSamePartitionId() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);

        write(k1);
        EventCompressionTestUtil.serialize(k2, 1).forEach(pair -> write(pair.getKey(), pair.getValue()));

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k1, k2));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = new Key(row, cf, "raw\u00001-2", viz, ts);
        SortedSet<Key> compressed = new TreeSet<>(List.of(c1));
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        Key c0 = new Key(row, cf, "raw\u00001-1", viz, ts);
        assertScanResults(results, list(k1, c0));
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedDataWithSamePartitionId() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);

        serialize(List.of(k1)).forEach(pair -> write(pair.getKey(), pair.getValue()));
        write(k2);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, list(k1, k2));

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = new Key(row, cf, "raw\u00001-2", viz, ts);
        SortedSet<Key> compressed = new TreeSet<>(List.of(c1));
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        Key c0 = new Key(row, cf, "raw\u00001-1", viz, ts);
        assertScanResults(results, list(k2, c0));
    }

    @Test
    @Override
    public void testLargeEventTriggersCompressionOfSerializedData() {
        List<Key> inputs = getLargeEvent();
        inputs.forEach(this::write);

        // verify serialization and deserialization work together
        SortedMap<Key,Value> results = scan();
        assertScanResults(results, inputs);

        // remove deserialization iterator and assert compressed results
        removeDeserializationIterator(tops, tableName);
        results = scan();

        Key c1 = new Key(row, cf, "gzip\u00001-36", viz, ts);
        SortedSet<Key> compressed = new TreeSet<>(List.of(c1));
        assertScanResults(results, list(compressed));

        // remove serialization iterator and assert original keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, inputs);
    }

    @Test
    @Override
    public void testLargeEventCompressedAfterLoweringTheCompressionThreshold() {
        removeDeserializationIterator(tops, tableName);
        List<Key> inputs = getLargeEvent();
        inputs.forEach(this::write);

        // set a high compression threshold so that the event is not compressed
        setCompressionThreshold(tops, tableName, 2048);
        SortedMap<Key,Value> results = scan();
        Key uncompressed = new Key(row, cf, "raw\u00001-36", viz, ts);
        assertScanResults(results, List.of(uncompressed));

        // lower the compression threshold and observe a compressed key
        setCompressionThreshold(tops, tableName, 256);
        results = scan();
        Key gzip = new Key(row, cf, "gzip\u00001-36", viz, ts);
        assertScanResults(results, list(gzip));
    }

    @Test
    @Override
    public void testLargeEventUncompressedAfterIncreasingTheCompressionThreshold() {
        removeDeserializationIterator(tops, tableName);
        List<Key> inputs = getLargeEvent();
        inputs.forEach(this::write);

        // set a high compression threshold so that the event is not compressed
        setCompressionThreshold(tops, tableName, 2048);
        SortedMap<Key,Value> results = scan();
        Key uncompressed = new Key(row, cf, "raw\u00001-36", viz, ts);
        assertScanResults(results, list(uncompressed));

        // lower the compression threshold and observe a compressed key
        setCompressionThreshold(tops, tableName, 256);
        results = scan();
        Key gzip = new Key(row, cf, "gzip\u00001-36", viz, ts);
        assertScanResults(results, list(gzip));
    }

    @Test
    @Override
    public void testLargeEventCompressionAlgorithmChanges() {
        removeDeserializationIterator(tops, tableName);
        List<Key> inputs = getLargeEvent();
        inputs.forEach(this::write);

        // verify compressed key contains the default algorithm for this test
        SortedMap<Key,Value> results = scan();
        Key gzip = new Key(row, cf, "gzip\u00001-36", viz, ts);
        assertScanResults(results, list(gzip));

        // change the compression algorithm and observe a different compressed key
        setCompressionAlgorithm(tops, tableName, "zstd");
        results = scan();
        Key zstd = new Key(row, cf, "zstd\u00001-36", viz, ts);
        assertScanResults(results, list(zstd));
    }

    protected List<Key> getLargeEvent() {
        int valuesPerField = 9;
        List<Key> inputs = new ArrayList<>();
        for (String field : List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D")) {
            for (int i = 0; i < valuesPerField; i++) {
                inputs.add(new Key(row, cf, field + "\0value-" + i, viz, ts));
            }
        }
        return inputs;
    }

    /**
     * Assert that the results of a scan are correct (i.e., no missing keys)
     *
     * @param results
     *            the map of results
     * @param expected
     *            the list of expected keys
     */
    protected void assertScanResults(SortedMap<Key,Value> results, List<Key> expected) {
        Iterator<Key> expectedIter = expected.iterator();
        for (Map.Entry<Key,Value> entry : results.entrySet()) {
            assertTrue(expectedIter.hasNext(), "more results than expected");
            assertEquals(expectedIter.next(), entry.getKey(), "key did not match expectation");
        }
        assertFalse(expectedIter.hasNext(), "did not find expected keys");

        assertRandomDirectSeeks(expected);
        assertRandomFollowingSeeks(expected);
    }

    /**
     * Verify that seeking to each expected key in random order returns the correct key.
     *
     * @param expected
     *            the list of expected keys
     */
    protected void assertRandomDirectSeeks(List<Key> expected) {
        List<Key> shuffled = shuffle(expected);
        for (Key key : shuffled) {
            Range range = new Range(key, true, null, false);
            Key result = seek(range);
            assertEquals(key, result);
        }
    }

    /**
     * Verify that seeking past expected keys in random order returns the correct key
     *
     * @param expected
     *            the list of expected keys
     */
    protected void assertRandomFollowingSeeks(List<Key> expected) {
        TreeSet<Key> sortedKeys = new TreeSet<>(expected);
        List<Key> shuffled = shuffle(expected);
        for (Key key : shuffled) {
            Key next = key.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            Range range = new Range(next, false, null, false);
            Key result = seek(range);
            Key following = sortedKeys.higher(next);
            if (following == null) {
                assertNull(result);
            } else {
                assertEquals(following, result);
            }
        }
    }
}
