package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.iterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addDeserializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.addSerializationIterator;
import static datawave.core.iterators.compress.event.EventCompressionTestUtil.setSerializationVersion;
import static datawave.query.RebuildingScannerTestHelper.INTERRUPT;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN;
import static datawave.query.RebuildingScannerTestHelper.getClient;
import static datawave.test.MacTestUtil.createOrRecreate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.iterators.compress.KeyGroup;
import datawave.data.hash.UID;
import datawave.query.function.KeyToDocumentData;
import datawave.query.iterator.SourceManagerTest.MockIteratorEnvironment;
import datawave.query.predicate.SeekingFilter;
import datawave.util.TableName;

public class EventDeserializationIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(EventDeserializationIteratorTest.class);

    private static final Value EMPTY_VALUE = new Value();
    private static final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");

    private final String row = "20251010_123";
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

    private void write(Collection<Key> keys) {
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

    private List<Pair<Key,Value>> scan() {
        return scan(auths);
    }

    private List<Pair<Key,Value>> scan(Authorizations authorizations) {
        AccumuloClient client = getRebuildingClient();
        try (Scanner scanner = client.createScanner(tableName, authorizations)) {
            scanner.setRange(new Range(row));

            List<Pair<Key,Value>> data = new ArrayList<>();
            for (Map.Entry<Key,Value> entry : scanner) {
                log.trace("key: {}", entry.getKey());
                data.add(Pair.of(entry.getKey(), entry.getValue()));
            }
            return data;
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
    public void testSimpleScan() throws IOException {
        List<Key> data = List.of(fiKey, eventKey, tfKey);
        SortedKeyValueIterator<Key,Value> source = iterator(data);

        EventDeserializationIterator iterator = new EventDeserializationIterator();
        iterator.init(source, Collections.emptyMap(), new MockIteratorEnvironment());
        iterator.seek(new Range(), Collections.emptySet(), false);

        while (iterator.hasTop()) {
            Key key = iterator.getTopKey();
            iterator.next();
        }
    }

    @Test
    public void testSerDeAllKeys() {
        write(fiKey, eventKey, tfKey);

        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);

        List<Pair<Key,Value>> results = scan();
        assertEquals(eventKey, results.get(0).getKey());
        assertEquals(fiKey, results.get(1).getKey());
        assertEquals(tfKey, results.get(2).getKey());
    }

    @Test
    public void testEachKeyWrittenWithDifferentVersion() throws IOException {
        Key k1 = new Key(row, "datatype\0" + uid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, "datatype\0" + uid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, "datatype\0" + uid, "FIELD_C\0value-c", "VIZ-A", ts);

        Pair<Key,Value> c1 = EventCompressionTestUtil.serialize(k1, 1).get(0);
        Pair<Key,Value> c2 = EventCompressionTestUtil.serialize(k2, 2).get(0);
        Pair<Key,Value> c3 = EventCompressionTestUtil.serialize(k3, 3).get(0);

        write(c1.getKey(), c1.getValue());
        write(c2.getKey(), c2.getValue());
        write(c3.getKey(), c3.getValue());

        // verify initial state
        List<Pair<Key,Value>> results = scan();
        assertEquals(3, results.size());
        assertEquals(EventMarkerUtil.createMarker(k1, "1", 1), results.get(0).getKey());
        assertEquals(EventMarkerUtil.createMarker(k2, "2", 1), results.get(1).getKey());
        assertEquals(EventMarkerUtil.createMarker(k3, "3", 1), results.get(2).getKey());

        addSerializationIterator(tops, tableName);
        addDeserializationIterator(tops, tableName);

        // verify that the serialization and deserialization iterator work together
        for (int index : List.of(1, 2, 3)) {
            setSerializationVersion(tops, tableName, index);
            results = scan();
            assertEquals(k1, results.get(0).getKey());
            assertEquals(k2, results.get(1).getKey());
            assertEquals(k3, results.get(2).getKey());
            assertEquals(3, results.size());
        }
    }

    @Test
    public void testKeyOrderMattersNot() throws IOException {
        int maxIterations = 100;
        for (int i = 0; i < maxIterations; i++) {
            createOrRecreate(tops, tableName);
            List<Key> keys = getRandomKeys();
            write(keys);

            addSerializationIterator(tops, tableName);

            List<Pair<Key,Value>> results = scan();
            Key compressed = new Key(row, "datatype\0" + uid, "raw\u00001-3", "VIZ-A", ts);
            assertEquals(compressed, results.get(0).getKey());
            assertEquals(1, results.size());

            addDeserializationIterator(tops, tableName);

            results = scan();
            assertEquals(keys.get(0), results.get(0).getKey());
            assertEquals(keys.get(1), results.get(1).getKey());
            assertEquals(keys.get(2), results.get(2).getKey());
            assertEquals(3, results.size());
        }
    }

    private List<Key> getRandomKeys() {
        List<String> fields = List.of("A", "B", "C", "D", "E");
        List<String> values = List.of("1", "2", "3", "4", "5");

        SecureRandom random = new SecureRandom();
        Set<Key> keys = new HashSet<>();
        while (keys.size() < 3) {
            String field = fields.get(random.nextInt(fields.size()));
            String value = values.get(random.nextInt(values.size()));
            keys.add(new Key(row, "datatype\0" + uid, field + '\u0000' + value, "VIZ-A", ts));
        }

        List<Key> keyList = new ArrayList<>(keys);
        Collections.sort(keyList);
        return keyList;
    }

    /**
     * The {@link KeyToDocumentData} function may utilize a seek range from a {@link SeekingFilter} when aggregating an event. For example, when applying limit
     * fields or an exclusion filter against a field that appears many times within a single event.
     * <p>
     * This test verifies that a seek range for a previous key correctly clears the buffer
     */
    @Test
    public void testSeekBeforeLargeEventFieldClearsDocumentBuffer() throws IOException {
        EventDeserializationIterator iterator = createIteratorForLargeEvent();

        // iterate through a few keys
        assertTrue(iterator.hasTop());
        for (int i = 0; i < 10; i++) {
            Key expected = new Key(row, "datatype\0" + uid, "FIELD_A\0value-" + (1000 + i), "VIZ-A", ts);
            assertEquals(expected, iterator.getTopKey());
            iterator.next();
        }

        // seek past the field
        Key start = new Key(row, "datatype\0" + uid, "FIELD_A\0\uFFFF");
        Key stop = new Key(row, "datatype\0" + uid + "\0");
        Range seekRange = new Range(start, false, stop, true);
        iterator.seek(seekRange, Collections.emptySet(), false);

        // verify no top after seek
        assertFalse(iterator.hasTop());
    }

    /**
     * The {@link KeyToDocumentData} function may utilize a seek range from a {@link SeekingFilter} when aggregating an event. For example, when applying limit
     * fields or an exclusion filter against a field that appears many times within a single event.
     * <p>
     * This test verifies that a seek range for the middle of the internal document buffer correctly unwinds the cached keys.
     */
    @Test
    public void testSeekWithinLargeEventFieldClearsDocumentBuffer() throws IOException {
        EventDeserializationIterator iterator = createIteratorForLargeEvent();

        // iterate through a few keys
        assertTrue(iterator.hasTop());
        for (int i = 0; i < 10; i++) {
            Key expected = new Key(row, "datatype\0" + uid, "FIELD_A\0value-" + (1000 + i), "VIZ-A", ts);
            assertEquals(expected, iterator.getTopKey());
            iterator.next();
        }

        // seek past the field
        Key start = new Key(row, "datatype\0" + uid, "FIELD_A\u0000value-1049\u0000");
        Key stop = new Key(row, "datatype\0" + uid + "\0");
        Range seekRange = new Range(start, false, stop, true);
        iterator.seek(seekRange, Collections.emptySet(), false);

        // verify no top after seek
        assertTrue(iterator.hasTop());
        Key expected = new Key(row, "datatype\0" + uid, "FIELD_A\0value-1050", "VIZ-A", ts);
        assertEquals(expected, iterator.getTopKey());
    }

    /**
     * The {@link KeyToDocumentData} function may utilize a seek range from a {@link SeekingFilter} when aggregating an event. For example, when applying limit
     * fields or an exclusion filter against a field that appears many times within a single event.
     * <p>
     * This test verifies that a seek range correctly skips the internal document buffer
     */
    @Test
    public void testSeekPastLargeEventFieldClearsDocumentBuffer() throws IOException {
        EventDeserializationIterator iterator = createIteratorForLargeEvent();

        // iterate through a few keys
        assertTrue(iterator.hasTop());
        for (int i = 0; i < 10; i++) {
            Key expected = new Key(row, "datatype\0" + uid, "FIELD_A\0value-" + (1000 + i), "VIZ-A", ts);
            assertEquals(expected, iterator.getTopKey());
            iterator.next();
        }

        // seek past the field
        Key start = new Key(row, "datatype\0" + uid, "FIELD_A\u0000\uFFFF");
        Key stop = new Key(row, "datatype\0" + uid + "\0");
        Range seekRange = new Range(start, false, stop, true);
        iterator.seek(seekRange, Collections.emptySet(), false);

        // verify no top after seek
        assertFalse(iterator.hasTop());
    }

    protected EventDeserializationIterator createIteratorForLargeEvent() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            String cf = "datatype\u0000" + uid;
            String cq = "FIELD_A\u0000value-" + (1000 + i);
            data.put(new Key(row, cf, cq, "VIZ-A", ts), EMPTY_VALUE);
        }

        // compress the event
        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(iterator(data));

        // verify document was compressed to a single key
        List<Pair<Key,Value>> compressedKeys = compressed.getKeyValues();
        assertEquals(1, compressedKeys.size());

        // now create the deserialization iterator
        EventDeserializationIterator iterator = new EventDeserializationIterator();
        iterator.init(iterator(data), Collections.emptyMap(), new MockIteratorEnvironment());
        iterator.seek(new Range(), Collections.emptySet(), false);
        return iterator;
    }
}
