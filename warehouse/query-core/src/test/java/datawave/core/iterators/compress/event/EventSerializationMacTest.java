package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.list;
import static datawave.core.iterators.compress.CompressionTestUtil.shuffle;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addDeserializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addSerializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.removeDeserializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.removeSerializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.serialize;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setCompressionAlgorithm;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setCompressionThreshold;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setSerializationVersion;
import static datawave.test.MacTestUtil.createOrRecreate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.hash.UID;
import datawave.util.TableName;

/**
 * For speed consider setting up different tables with different iterator configs
 */
public class EventSerializationMacTest implements EventSerializationTestCases {

    private static final Logger log = LoggerFactory.getLogger(EventSerializationMacTest.class);

    private static final Value EMPTY_VALUE = new Value();

    @TempDir
    public static Path temporaryFolder;

    private static final String PASSWORD = "password";
    private static final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");

    private static MiniAccumuloCluster mac;
    private static AccumuloClient client;
    private static TableOperations tops;

    private final long ts = Instant.parse("2011-12-03T10:15:30Z").toEpochMilli();
    private final String uid = UID.builder().newId("abc.def.ghi".getBytes(), (Date) null).toString();

    private final String tableName = TableName.SHARD;
    private final String row = "20251010_123";
    private final String cf = "datatype\0" + uid;
    private final String cq = "FIELD_A\0value-a";
    private final String viz = "VIZ-A";

    // some default keys found in the shard table
    Key fiKey = new Key(row, "fi\0FIELD_A", "value-a\0datatype\0" + uid, "VIZ-A", ts);
    Key eventKey = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
    Key tfKey = new Key(row, "tf", "datatype\0" + uid + "value-a\0FIELD_A", "VIZ-A", ts);

    @BeforeAll
    public static void beforeAll() throws Exception {
        MiniAccumuloConfig config = new MiniAccumuloConfig(temporaryFolder.toFile(), PASSWORD);
        config.setNumTservers(1);

        mac = new MiniAccumuloCluster(config);
        mac.start();

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
        tops = client.tableOperations();

        client.securityOperations().changeUserAuthorizations("root", auths);
    }

    @BeforeEach
    public void beforeEach() {
        createOrRecreate(tops, tableName);
        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        mac.stop();
    }

    public void compactRow() {
        try {
            tops.compact(tableName, new Text(row), new Text(row + "~"), true, true);
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            fail("Failed to compact shard table", e);
            throw new RuntimeException(e);
        }
    }

    protected void write(Key... keys) {
        for (Key key : keys) {
            write(key, EMPTY_VALUE);
        }
    }

    protected void write(Key key, Value value) {
        write(key.getRow().toString(), key.getColumnFamily().toString(), key.getColumnQualifier().toString(), key.getColumnVisibility().toString(),
                        key.getTimestamp(), value);
    }

    protected void write(String row, String cf, String cq, String cv, long ts, Value value) {
        try (var bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(row);
            m.put(cf, cq, new ColumnVisibility(cv), ts, value);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail("Failed to write mutation: " + row + " " + cf + " " + cq + " " + cv);
            throw new RuntimeException(e);
        }
    }

    protected void delete(Key... keys) {
        for (Key key : keys) {
            delete(key);
        }
    }

    protected void delete(Key key) {
        delete(key.getRow().toString(), key.getColumnFamily().toString(), key.getColumnQualifier().toString(), key.getColumnVisibility().toString(),
                        key.getTimestamp());
    }

    protected void delete(String row, String cf, String cq, String cv, long ts) {
        try (var bw = client.createBatchWriter(tableName)) {
            Mutation m = new Mutation(row);
            m.putDelete(cf, cq, new ColumnVisibility(cv), ts);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            fail("Failed to write mutation: " + row + " " + cf + " " + cq + " " + cv);
            throw new RuntimeException(e);
        }
    }

    protected List<Map.Entry<Key,Value>> scan() {
        return scan(row);
    }

    protected List<Map.Entry<Key,Value>> scan(String row) {
        try (var scanner = client.createScanner(tableName, auths)) {
            Range range = new Range(row);
            scanner.setRange(range);

            List<Map.Entry<Key,Value>> results = new ArrayList<>();
            for (Map.Entry<Key,Value> entry : scanner) {
                results.add(entry);
            }
            return results;
        } catch (TableNotFoundException e) {
            fail("Failed to scan shard table", e);
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

    @Test
    @Override
    public void testOneOfEachKeyType() {
        Key fiKey = new Key(row, "fi\0FIELD_A", "value-a\0datatype\0" + uid, "VIZ-A", ts);
        Key eventKey = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key tfKey = new Key(row, "tf", "datatype\0" + uid + "value-a\0FIELD_A", "VIZ-A", ts);

        // write keys and assert initial scan with both serialization and deserialization
        write(fiKey, eventKey, tfKey);
        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(fiKey, eventKey, tfKey));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(fiKey, eventKey, tfKey));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = EventMarkerUtil.createMarker(eventKey, "1", 1);
        assertScanResults(results, list(fiKey, compressed, tfKey));
    }

    @Test
    @Override
    public void testDifferentVisibilities() {
        Key k1 = new Key(row, cf, cq, "VIZ-A", ts);
        Key k2 = new Key(row, cf, cq, "VIZ-B", ts);
        Key k3 = new Key(row, cf, cq, "VIZ-C", ts);

        // write keys and assert initial scan with both serialization and deserialization
        write(k1, k2, k3);
        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key c1 = EventMarkerUtil.createMarker(k1, "1", 1);
        Key c2 = EventMarkerUtil.createMarker(k2, "1", 1);
        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        assertScanResults(results, list(c1, c2, c3));
    }

    /**
     * The iterators correctly produce keys per distinct timestamp, versioning iterator keeps the latest
     */
    @Test
    @Override
    public void testDifferentTimestamps() {
        Key k1 = new Key(row, cf, cq, viz, 10);
        Key k2 = new Key(row, cf, cq, viz, 11);
        Key k3 = new Key(row, cf, cq, viz, 12);

        // write keys and assert initial scan with both serialization and deserialization
        write(k1, k2, k3);
        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k3));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k3));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key c3 = EventMarkerUtil.createMarker(k3, "1", 1);
        assertScanResults(results, list(c3));
    }

    @Test
    @Override
    public void testDifferentDeleteFlags() {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, false);
        Key d1 = new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), viz.getBytes(), ts, true);
        write(k1);
        delete(d1);

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, Collections.emptyList());

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, Collections.emptyList());

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
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

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(event));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = new Key(row, cf, "raw\u00001-9", viz, ts);
        assertScanResults(results, List.of(compressed));
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

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(event));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        Key compressed = new Key(row, cf, "raw\u00001-12", viz, ts);
        assertScanResults(results, List.of(compressed));
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

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(event));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-9", viz, ts));
        }
        assertScanResults(results, list(compressed));
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

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(event));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(event));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        SortedSet<Key> compressed = new TreeSet<>();
        compressed.add(new Key(row, cf, "raw\u00001-9", viz, ts));
        for (String child : children) {
            compressed.add(new Key(row, cf + child, "raw\u00001-18", viz, ts));
        }
        assertScanResults(results, list(compressed));
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedData() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);
        Key c1 = new Key(row, cf, "raw\u00001-3", viz, ts);

        write(k1, k2);
        serialize(List.of(k3)).forEach(pair -> write(pair.getKey(), pair.getValue()));

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(c1));
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedData() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key k3 = new Key(row, cf, "FIELD_C\0value-c", viz, ts);
        Key c1 = new Key(row, cf, "raw\u00001-3", viz, ts);

        serialize(List.of(k1, k2)).forEach(pair -> write(pair.getKey(), pair.getValue()));
        write(k3);

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k1, k2, k3));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(c1));
    }

    @Test
    @Override
    public void testLatentLoadOfCompressedDataWithSamePartitionId() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key c1 = new Key(row, cf, "raw\u00001-2", viz, ts);

        write(k1);
        serialize(List.of(k2)).forEach(pair -> write(pair.getKey(), pair.getValue()));

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k1, k2));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k1, k2));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(c1));
    }

    @Test
    @Override
    public void testLatentLoadOfUncompressedDataWithSamePartitionId() {
        Key k1 = new Key(row, cf, "FIELD_A\0value-a", viz, ts);
        Key k2 = new Key(row, cf, "FIELD_B\0value-b", viz, ts);
        Key c1 = new Key(row, cf, "raw\u00001-2", viz, ts);

        serialize(List.of(k1)).forEach(pair -> write(pair.getKey(), pair.getValue()));
        write(k2);

        List<Map.Entry<Key,Value>> results = scan();
        assertScanResults(results, list(k1, k2));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(k1, k2));

        // remove deserialization iterator and verify compressed event keys
        removeDeserializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(c1));
    }

    @Test
    @Override
    public void testLargeEventTriggersCompressionOfSerializedData() {
        SortedSet<Key> event = getLargeEvent();
        event.forEach(this::write);
        removeDeserializationIterator(tops, tableName);

        List<Map.Entry<Key,Value>> results = scan();
        Key compressed = new Key(row, cf, "gzip\u00001-100", viz, ts);
        assertScanResults(results, list(compressed));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(compressed));

        // remove serialization iterator and verify compressed event keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(compressed));
    }

    @Test
    @Override
    public void testLargeEventCompressedAfterLoweringTheCompressionThreshold() {
        SortedSet<Key> event = getLargeEvent();
        event.forEach(this::write);
        removeDeserializationIterator(tops, tableName);

        // large threshold should return a raw key
        setCompressionThreshold(tops, tableName, 2048);
        List<Map.Entry<Key,Value>> results = scan();
        Key raw = new Key(row, cf, "raw\u00001-100", viz, ts);
        assertScanResults(results, list(raw));

        // lower threshold should return a compressed marker
        setCompressionThreshold(tops, tableName, 256);
        results = scan();
        Key compressed = new Key(row, cf, "gzip\u00001-100", viz, ts);
        assertScanResults(results, list(compressed));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(compressed));

        // remove serialization iterator and verify compressed event keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(compressed));
    }

    @Test
    @Override
    public void testLargeEventUncompressedAfterIncreasingTheCompressionThreshold() {

    }

    @Test
    @Override
    public void testLargeEventCompressionAlgorithmChanges() {
        SortedSet<Key> event = getLargeEvent();
        event.forEach(this::write);
        removeDeserializationIterator(tops, tableName);

        // initial scan should return gzip marker
        List<Map.Entry<Key,Value>> results = scan();
        Key compressed = new Key(row, cf, "gzip\u00001-100", viz, ts);
        assertScanResults(results, list(compressed));

        // different algorithm should return different marker
        setCompressionAlgorithm(tops, tableName, EventSerializationUtil.ZSTD);
        results = scan();
        Key zstd = new Key(row, cf, "zstd\u00001-100", viz, ts);
        assertScanResults(results, list(zstd));

        // compact the underlying data and assert same results
        compactRow();
        results = scan();
        assertScanResults(results, list(zstd));

        // remove serialization iterator and verify compressed event keys
        removeSerializationIterator(tops, tableName);
        results = scan();
        assertScanResults(results, List.of(zstd));
    }

    protected SortedSet<Key> getLargeEvent() {
        int valuesPerField = 20;
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E");

        SortedSet<Key> event = new TreeSet<>();
        for (String field : fields) {
            for (int i = 0; i < valuesPerField; i++) {
                String cq = field + "\0value-" + i;
                Key key = new Key(row, cf, cq, viz, ts);
                event.add(key);
            }
        }
        return event;
    }

    @Test
    public void testStandardCreateReadCompactRead() {
        removeSerializationIterator(tops, tableName);

        // write a field index, event, and term frequency key
        write(fiKey, EMPTY_VALUE);
        write(eventKey, EMPTY_VALUE);
        write(tfKey, EMPTY_VALUE);

        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(eventKey, results.get(0).getKey());
        assertEquals(fiKey, results.get(1).getKey());
        assertEquals(tfKey, results.get(2).getKey());
        assertEquals(3, results.size());

        compactRow();

        results = scan(row);
        assertEquals(eventKey, results.get(0).getKey());
        assertEquals(fiKey, results.get(1).getKey());
        assertEquals(tfKey, results.get(2).getKey());
        assertEquals(3, results.size());
    }

    @Test
    public void testCompressedCreateReadCompactRead() {
        removeSerializationIterator(tops, tableName);

        // write a field index, event, and term frequency key
        write(fiKey, EMPTY_VALUE);
        write(eventKey, EMPTY_VALUE);
        write(tfKey, EMPTY_VALUE);

        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(eventKey, results.get(0).getKey());
        assertEquals(fiKey, results.get(1).getKey());
        assertEquals(tfKey, results.get(2).getKey());
        assertEquals(3, results.size());

        addSerializationIterator(tops, tableName);
        compactRow();

        removeDeserializationIterator(tops, tableName);
        results = scan(row);

        Key expectedEventKey = EventMarkerUtil.createMarker(eventKey, "1", 1);
        assertEquals(expectedEventKey, results.get(0).getKey());
        assertEquals(fiKey, results.get(1).getKey());
        assertEquals(tfKey, results.get(2).getKey());
        assertEquals(3, results.size());
    }

    @Test
    public void testCompactKeysWithDifferentVersions() {
        Key k1 = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, "datatype\0" + uid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, "datatype\0" + uid, "FIELD_C\0value-c", "VIZ-A", ts);
        write(k1, k2, k3);

        for (int i : List.of(1, 2, 3)) {
            setSerializationVersion(tops, tableName, i);
            compactRow();

            List<Map.Entry<Key,Value>> results = scan(row);
            assertEquals(k1, results.get(0).getKey());
            assertEquals(k2, results.get(1).getKey());
            assertEquals(k3, results.get(2).getKey());
        }
    }

    @Test
    public void testSimulateIngestWithDifferentVersions() {
        Key k1 = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, "datatype\0" + uid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, "datatype\0" + uid, "FIELD_C\0value-c", "VIZ-A", ts);
        Key k4 = new Key(row, "datatype\0" + uid, "FIELD_D\0value-d", "VIZ-A", ts);

        Pair<Key,Value> p1 = EventCompressionTestUtil.serialize(k2, 1).get(0);
        Pair<Key,Value> p2 = EventCompressionTestUtil.serialize(k3, 2).get(0);
        Pair<Key,Value> p3 = EventCompressionTestUtil.serialize(k4, 3).get(0);

        write(k1);
        for (Pair<Key,Value> pair : List.of(p1, p2, p3)) {
            write(pair.getKey(), pair.getValue());
        }

        // assert raw scan
        removeDeserializationIterator(tops, tableName);
        removeSerializationIterator(tops, tableName);
        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(4, results.size());
        assertEquals(k1, results.get(0).getKey());
        assertEquals(p1.getKey(), results.get(1).getKey());
        assertEquals(p2.getKey(), results.get(2).getKey());
        assertEquals(p3.getKey(), results.get(3).getKey());

        // verify the serialization version in the iterator can handle any input version (raw, 1, 2, 3)
        addSerializationIterator(tops, tableName);
        for (int version : List.of(1, 2, 3)) {
            setSerializationVersion(tops, tableName, version);
            results = scan(row);
            Key expected = new Key(row, "datatype\0" + uid, "raw\0" + version + "-4", "VIZ-A", ts);
            assertEquals(expected, results.get(0).getKey());
            assertEquals(1, results.size());
        }

        // verify that the underlying data was not changed
        removeSerializationIterator(tops, tableName);

        results = scan(row);
        assertEquals(k1, results.get(0).getKey());
        assertEquals(p1.getKey(), results.get(1).getKey());
        assertEquals(p2.getKey(), results.get(2).getKey());
        assertEquals(p3.getKey(), results.get(3).getKey());
        assertEquals(4, results.size());
    }

    @Test
    public void testRepeatedCompactionsUpdateVersion() {
        Key k1 = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, "datatype\0" + uid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, "datatype\0" + uid, "FIELD_C\0value-c", "VIZ-A", ts);
        Key k4 = new Key(row, "datatype\0" + uid, "FIELD_D\0value-d", "VIZ-A", ts);
        write(k1, k2, k3, k4);

        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(k1, results.get(0).getKey());
        assertEquals(k2, results.get(1).getKey());
        assertEquals(k3, results.get(2).getKey());
        assertEquals(k4, results.get(3).getKey());

        for (int version : List.of(1, 2, 3)) {
            // ensure the compaction uses the correct version
            addSerializationIterator(tops, tableName);
            setSerializationVersion(tops, tableName, version);
            compactRow();

            // verify underlying data was compacted with the version by doing a 'raw' scan
            removeDeserializationIterator(tops, tableName);
            results = scan(row);
            Key expected = new Key(row, "datatype\0" + uid, "raw\0" + version + "-4", "VIZ-A", ts);
            assertEquals(expected, results.get(0).getKey());
            assertEquals(1, results.size());
        }
    }

    @Test
    public void testRepeatedLoadAndCompact() {
        Key k1 = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, "datatype\0" + uid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, "datatype\0" + uid, "FIELD_C\0value-c", "VIZ-A", ts);
        Key k4 = new Key(row, "datatype\0" + uid, "FIELD_D\0value-d", "VIZ-A", ts);

        write(k1, k2);
        setSerializationVersion(tops, tableName, 1);
        compactRow();

        write(k1, k3);
        setSerializationVersion(tops, tableName, 2);
        compactRow();

        write(k1, k4);
        setSerializationVersion(tops, tableName, 3);
        compactRow();

        // verify raw key
        removeDeserializationIterator(tops, tableName);
        List<Map.Entry<Key,Value>> results = scan(row);
        Key expected = new Key(row, "datatype\0" + uid, "raw\u00003-6", "VIZ-A", ts);
        assertEquals(expected, results.get(0).getKey());
        assertEquals(1, results.size());

        // verify deserialized keys
        addDeserializationIterator(tops, tableName);
        results = scan(row);
        assertEquals(4, results.size());
        assertEquals(k1, results.get(0).getKey());
        assertEquals(k2, results.get(1).getKey());
        assertEquals(k3, results.get(2).getKey());
        assertEquals(k4, results.get(3).getKey());
    }

    @Test
    public void testVanillaDeleteMarker() {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), ts, false);
        Key k2 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), ts, true);

        write(k1);
        delete(k2);
        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(0, results.size());
    }

    @Test
    public void testWriteCompactAndThenDelete() {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), ts, false);
        Key k2 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), ts, true);

        write(k1);
        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);
        compactRow();

        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(k1, results.get(0).getKey());
        assertEquals(1, results.size());

        delete(k2);
        compactRow();

        results = scan(row);
        assertEquals(0, results.size());
    }

    /**
     * This test verifies that fine-grain control is retained even with compressed documents
     */
    @Disabled
    @Test
    public void testDocumentPartiallyDeleted() {
        Key k1 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-a".getBytes(), "VIZ-A".getBytes(), ts, false);
        Key k2 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-b".getBytes(), "VIZ-A".getBytes(), ts, false);
        Key k3 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-c".getBytes(), "VIZ-A".getBytes(), ts, false);

        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);

        write(k1, k2, k3);
        compactRow();

        List<Map.Entry<Key,Value>> results = scan(row);
        assertEquals(k1, results.get(0).getKey());
        assertEquals(k2, results.get(1).getKey());
        assertEquals(k3, results.get(2).getKey());
        assertEquals(3, results.size());

        Key k4 = new Key(row.getBytes(), cf.getBytes(), "FIELD_A\0value-b".getBytes(), "VIZ-A".getBytes(), ts, true);
        delete(k4);
        compactRow();

        results = scan(row);
        assertEquals(k1, results.get(0).getKey());
        assertEquals(k3, results.get(1).getKey());
        assertEquals(2, results.size());
    }

    protected void assertScanResults(List<Map.Entry<Key,Value>> results, List<Key> expected) {
        Iterator<Key> expectedIter = expected.iterator();
        for (Map.Entry<Key,Value> entry : results) {
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
