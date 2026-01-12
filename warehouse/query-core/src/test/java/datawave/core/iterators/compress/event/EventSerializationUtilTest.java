package datawave.core.iterators.compress.event;

import static datawave.core.iterators.compress.CompressionTestUtil.iterator;
import static datawave.core.iterators.compress.CompressionTestUtil.toSortedMapFromPairs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.iterators.compress.KeyGroup;
import datawave.core.iterators.compress.MetaKeySerializer;

public class EventSerializationUtilTest {

    private static final Logger log = LoggerFactory.getLogger(EventSerializationUtilTest.class);

    private static final Value EMPTY_VALUE = new Value();

    private final String row = "row";
    private final String dtUid = "datatype\0uid";
    private final long ts = 10L;

    @Test
    public void testKeyIsCompressedMarker() {
        Key normal = new Key("row", "datatype\u0000uid", "FIELD\u0000value", "VIZ-A", 10L);
        assertFalse(EventMarkerUtil.isMarker(normal));

        Key compressed = new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 10L);
        assertTrue(EventMarkerUtil.isMarker(compressed));
    }

    @Test
    public void testTextIsCompressedMarker() {
        Text normal = new Text("FIELD\u0000value");
        assertFalse(EventMarkerUtil.isMarker(normal));

        Text compressed = new Text("raw\u00001-1");
        assertTrue(EventMarkerUtil.isMarker(compressed));
    }

    @Test
    public void testStringIsCompressedMarker() {
        String normal = "FIELD\u0000value";
        assertFalse(EventMarkerUtil.isMarker(normal));

        String compressed = "raw\u00001-1";
        assertTrue(EventMarkerUtil.isMarker(compressed));
    }

    @Test
    public void testMalformedEventKeyIsMarker() {
        Key malformedCF = new Key("row", "datatypeuid", "FIELD\u0000value", "VIZ-A", 10L);
        assertFalse(EventMarkerUtil.isMarker(malformedCF));

        Key malformedCQ = new Key("row", "datatype\u0000uid", "FIELDvalue", "VIZ-A", 10L);
        assertFalse(EventMarkerUtil.isMarker(malformedCQ));
    }

    @Test
    public void testCreateCompressedMarker() {
        Key normal = new Key("row", "datatype\u0000uid", "FIELD\u0000value", "VIZ-A", 10L);
        Key expected = new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 10L);

        Key compressed = EventMarkerUtil.createMarker(normal, "1", 1);
        assertEquals(expected, compressed);
    }

    @Test
    public void testIsEventKey() {
        Key eventKey = new Key("row", "datatype\u0000uid", "FIELD\u0000value", "VIZ-A", 10L);
        assertTrue(EventSerializationUtil.isEventKey(eventKey));

        Key malformedCF = new Key("row", "datatypeuid", "FIELD\u0000value", "VIZ-A", 10L);
        assertTrue(EventSerializationUtil.isEventKey(malformedCF));

        Key malformedCQ = new Key("row", "datatype\u0000uid", "FIELDvalue", "VIZ-A", 10L);
        assertTrue(EventSerializationUtil.isEventKey(malformedCQ));
    }

    @Test
    public void testIteratorToValue() throws IOException {
        SortedKeyValueIterator<Key,Value> iter = iterator(documentOne());

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(iter);

        Key expectedMarker = new Key("row", "datatype\u0000uid", "raw\u00001-5", "VIZ-A", 10L);
        int expectedValueLength = 77;

        List<Pair<Key,Value>> data = compressed.getKeyValues();
        assertEquals(1, data.size());
        assertEquals(expectedMarker, data.get(0).getKey());
        assertEquals(expectedValueLength, data.get(0).getValue().get().length);
    }

    /**
     * A test that verifies the same document can be serialized via many compression algorithms and still be deserialized by the {@link MetaKeySerializer}
     */
    @Test
    public void testVersionMultiplexing() throws IOException {
        Key k1 = new Key(row, dtUid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, dtUid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, dtUid, "FIELD_C\0value-c", "VIZ-A", ts);

        TreeMap<Key,Value> data = new TreeMap<>();
        int version = 1;
        for (Key k : List.of(k1, k2, k3)) {
            Pair<Key,Value> compressedPair = EventCompressionTestUtil.serialize(k, version++).get(0);
            data.put(compressedPair.getKey(), compressedPair.getValue());
        }

        // given variety of input versions, verify that output is a single version
        for (int compressedVersion : List.of(1, 2, 3)) {
            // assert compressed results
            EventSerializationUtil util = new EventSerializationUtil();
            util.setSerializationVersion(compressedVersion);
            KeyGroup compressed = util.serialize(iterator(data));
            List<Pair<Key,Value>> pairs = compressed.getKeyValues();
            assertEquals(1, pairs.size());
            Key expectedMarker = EventMarkerUtil.createMarker(k3, String.valueOf(compressedVersion), 3);
            assertEquals(expectedMarker, pairs.get(0).getKey());

            // assert all original inputs were preserved
            Iterator<Map.Entry<Key,Value>> resultIter = util.deserialize(iterator(toSortedMapFromPairs(pairs)));
            assertEquals(k1, resultIter.next().getKey());
            assertEquals(k2, resultIter.next().getKey());
            assertEquals(k3, resultIter.next().getKey());
            assertFalse(resultIter.hasNext());
        }
    }

    /**
     * A test that verifies the same document can be serialized via many compression algorithms and still be deserialized by the {@link MetaKeySerializer}.
     * <p>
     * Similar to {@link #testVersionMultiplexing()} but the compression versions are ordered such that the compressed sort order is the inverse of of the raw
     * keys.
     */
    @Test
    public void testInverseVersionMultiplexing() throws IOException {
        Key k1 = new Key(row, dtUid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, dtUid, "FIELD_B\0value-b", "VIZ-A", ts);
        Key k3 = new Key(row, dtUid, "FIELD_C\0value-c", "VIZ-A", ts);

        TreeMap<Key,Value> data = new TreeMap<>();
        int version = 1;
        for (Key k : List.of(k3, k2, k1)) {
            Pair<Key,Value> compressedPair = EventCompressionTestUtil.serialize(k, version++).get(0);
            data.put(compressedPair.getKey(), compressedPair.getValue());
        }

        // given variety of input versions, verify that output is a single version
        for (int compressedVersion : List.of(3, 2, 1)) {
            // assert compressed results
            EventSerializationUtil util = new EventSerializationUtil();
            util.setSerializationVersion(compressedVersion);
            KeyGroup compressed = util.serialize(iterator(data));
            List<Pair<Key,Value>> pairs = compressed.getKeyValues();
            assertEquals(1, pairs.size());
            Key expectedMarker = EventMarkerUtil.createMarker(k3, String.valueOf(compressedVersion), 3);
            assertEquals(expectedMarker, pairs.get(0).getKey());

            // assert all original inputs were preserved
            Iterator<Map.Entry<Key,Value>> resultIter = util.deserialize(iterator(toSortedMapFromPairs(pairs)));
            assertEquals(k1, resultIter.next().getKey());
            assertEquals(k2, resultIter.next().getKey());
            assertEquals(k3, resultIter.next().getKey());
            assertFalse(resultIter.hasNext());
        }
    }

    @Test
    public void testDocumentWithAndWithoutCompression() throws IOException {
        Key k1 = new Key(row, dtUid, "FIELD_A\0value-a", "VIZ-A", ts);
        Key k2 = new Key(row, dtUid, "FIELD_B\0value-b", "VIZ-A", ts);

        // k1 is compressed, k2 is not
        TreeMap<Key,Value> data = new TreeMap<>();
        Pair<Key,Value> compressedPair = EventCompressionTestUtil.serialize(k1);
        data.put(compressedPair.getKey(), compressedPair.getValue());
        data.put(k2, EMPTY_VALUE);

        // utility should handle merging disjoint compressed docs
        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(iterator(data));
        List<Pair<Key,Value>> pairs = compressed.getKeyValues();
        assertEquals(1, pairs.size());
        assertEquals(EventMarkerUtil.createMarker(k1, "1", 2), pairs.get(0).getKey());

        // assert initial state given the compressed data
        Iterator<Map.Entry<Key,Value>> iter = util.deserialize(iterator(toSortedMapFromPairs(pairs)));
        assertTrue(iter.hasNext());
        assertEquals(k1, iter.next().getKey());
        assertTrue(iter.hasNext());
        assertEquals(k2, iter.next().getKey());
        assertFalse(iter.hasNext());
    }

    private SortedMap<Key,Value> documentOne() {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-2", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-3", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_B\u0000value-4", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_C\u0000value-5", "VIZ-A", 10L), EMPTY_VALUE);
        return data;
    }

    @Test
    public void testDocumentWithMixOfVisibilities() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_B\u0000value-2", "VIZ-A", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_C\u0000value-3", "VIZ-B", 10L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_D\u0000value-4", "VIZ-B", 10L), EMPTY_VALUE);

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        source.seek(new Range(), Collections.emptySet(), false);

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(source);
        List<Pair<Key,Value>> pairs = compressed.getKeyValues();

        assertEquals(2, pairs.size());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-2", "VIZ-A", 10L), pairs.get(0).getKey());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-2", "VIZ-B", 10L), pairs.get(1).getKey());
    }

    @Test
    public void testDocumentWithMixOfTimestamps() throws IOException {
        List<Key> data = new ArrayList<>();
        data.add(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-A", 11L));
        data.add(new Key("row", "datatype\u0000uid", "FIELD_B\u0000value-2", "VIZ-A", 10L));

        SortedKeyValueIterator<Key,Value> source = iterator(data);

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(source);
        List<Pair<Key,Value>> pairs = compressed.getKeyValues();

        assertEquals(2, pairs.size());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 11L), pairs.get(0).getKey());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 10L), pairs.get(1).getKey());
    }

    @Test
    public void testDocumentWithAdditionalVisibilitiesAndTimestamps() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-A", 12L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-A", 11L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-B", 11L), EMPTY_VALUE);
        data.put(new Key("row", "datatype\u0000uid", "FIELD_A\u0000value-1", "VIZ-B", 10L), EMPTY_VALUE);

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        source.seek(new Range(), Collections.emptySet(), false);

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(source);
        List<Pair<Key,Value>> pairs = compressed.getKeyValues();

        assertEquals(4, pairs.size());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 12L), pairs.get(0).getKey());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-A", 11L), pairs.get(1).getKey());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-B", 11L), pairs.get(2).getKey());
        assertEquals(new Key("row", "datatype\u0000uid", "raw\u00001-1", "VIZ-B", 10L), pairs.get(3).getKey());
    }

    @Test
    public void testDeleteMarkers() throws IOException {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("row".getBytes(), "datatype\u0000uid".getBytes(), "FIELD_A\u0000value-1".getBytes(), "VIZ-A".getBytes(), 10L, false), EMPTY_VALUE);
        data.put(new Key("row".getBytes(), "datatype\u0000uid".getBytes(), "FIELD_A\u0000value-1".getBytes(), "VIZ-A".getBytes(), 10L, true), EMPTY_VALUE);

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);
        source.seek(new Range(), Collections.emptySet(), false);

        EventSerializationUtil util = new EventSerializationUtil();
        KeyGroup compressed = util.serialize(source);
        List<Pair<Key,Value>> pairs = compressed.getKeyValues();

        // delete sorts first
        assertEquals(2, pairs.size());
        assertEquals(new Key("row".getBytes(), "datatype\u0000uid".getBytes(), "raw\u00001-1".getBytes(), "VIZ-A".getBytes(), 10L, true),
                        pairs.get(0).getKey());
        assertEquals(new Key("row".getBytes(), "datatype\u0000uid".getBytes(), "raw\u00001-1".getBytes(), "VIZ-A".getBytes(), 10L, false),
                        pairs.get(1).getKey());
    }

    @Test
    public void testDocumentWithOverlappingKeys() {
        // this might need to happen in an integration test
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRepeatedCompression() throws IOException {
        Key key = new Key("row", "datatype\u0000uid", "FIELD_A\u0000value                                with spaces", "VIZ-A", 10L);
        SortedKeyValueIterator<Key,Value> source = iterator(List.of(key));

        EventSerializationUtil util = new EventSerializationUtil();
        util.setSerializationVersion(1);
        util.setCompressionThreshold(5);
        util.setCompressionAlgorithm(EventSerializationUtil.GZIP);

        // compress key and verify algorithm in marker key
        KeyGroup group = util.serialize(source);
        Key expectedGzipMarker = new Key("row", "datatype\u0000uid", "gzip\u00001-1", "VIZ-A", 10L);
        assertEquals(1, group.getKeyValues().size());
        assertEquals(expectedGzipMarker, group.getKeyValues().get(0).getKey());

        // verify that compressing an already-compressed key works correctly
        source = iterator(group.getKeyValues().get(0));
        group = util.serialize(source);
        assertEquals(1, group.getKeyValues().size());
        assertEquals(expectedGzipMarker, group.getKeyValues().get(0).getKey());

        // verify that compressing an already-compressed key works correctly
        util.setCompressionAlgorithm(EventSerializationUtil.ZSTD);
        source = iterator(group.getKeyValues().get(0));
        group = util.serialize(source);
        Key expectedZstdMarker = new Key("row", "datatype\u0000uid", "zstd\u00001-1", "VIZ-A", 10L);
        assertEquals(1, group.getKeyValues().size());
        assertEquals(expectedZstdMarker, group.getKeyValues().get(0).getKey());

        // verify that we can repeat compress ZSTD
        source = iterator(group.getKeyValues().get(0));
        group = util.serialize(source);
        assertEquals(1, group.getKeyValues().size());
        assertEquals(expectedZstdMarker, group.getKeyValues().get(0).getKey());
    }

    @Test
    public void testDecompressionRequiredMultipleKeys() {
        List<Pair<Key,Value>> pairs = new ArrayList<>();
        pairs.add(Pair.of(new Key(), new Value()));
        pairs.add(Pair.of(new Key(), new Value()));

        EventSerializationUtil util = new EventSerializationUtil();
        assertTrue(util.isDecompressionRequired(pairs));
    }

    @Test
    public void testDecompressionRequiredSerializationVersionChanged() {
        Key key = new Key("row", "cf", "raw\u00001-1", "VIZ-A", 10L);
        List<Pair<Key,Value>> pairs = new ArrayList<>();
        pairs.add(Pair.of(key, new Value()));

        EventSerializationUtil util = new EventSerializationUtil();
        util.setSerializationVersion(2);
        assertTrue(util.isDecompressionRequired(pairs));
    }

    @Test
    public void testDecompressionRequiredCompressionAlgorithmChanged() {
        Key raw = new Key("row", "cf", "raw\u00001-1", "VIZ-A", 10L);
        List<Pair<Key,Value>> pairs = List.of(Pair.of(raw, new Value()));

        EventSerializationUtil util = new EventSerializationUtil();
        assertDecompressionRequired(util, EventSerializationUtil.RAW, pairs, false);
        assertDecompressionRequired(util, EventSerializationUtil.GZIP, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.ZSTD, pairs, true);

        Key gzip = new Key("row", "cf", "gzip\u00001-1", "VIZ-A", 10L);
        pairs = List.of(Pair.of(gzip, new Value()));
        assertDecompressionRequired(util, EventSerializationUtil.RAW, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.GZIP, pairs, false);
        assertDecompressionRequired(util, EventSerializationUtil.ZSTD, pairs, true);

        Key zstd = new Key("row", "cf", "zstd\u00001-1", "VIZ-A", 10L);
        pairs = List.of(Pair.of(zstd, new Value()));
        assertDecompressionRequired(util, EventSerializationUtil.RAW, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.GZIP, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.ZSTD, pairs, false);
    }

    @Test
    public void testDecompressionRequiredMultipleKeyValuePairs() {
        Key zstdOne = new Key("row", "cf", "zstd\u00001-2", "VIZ-A", 10L);
        Key zstdTwo = new Key("row", "cf", "zstd\u00001-3", "VIZ-A", 10L);
        List<Pair<Key,Value>> pairs = new ArrayList<>();
        pairs.add(Pair.of(zstdOne, new Value()));
        pairs.add(Pair.of(zstdTwo, new Value()));

        // decompression always required, even when the algorithm matches because the values need to be merged
        EventSerializationUtil util = new EventSerializationUtil();
        assertDecompressionRequired(util, EventSerializationUtil.RAW, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.GZIP, pairs, true);
        assertDecompressionRequired(util, EventSerializationUtil.ZSTD, pairs, true);
    }

    @Test
    public void testDecompressionRequiredSerializationVersionChanges() {
        Key zstdOne = new Key("row", "cf", "zstd\u00001-2", "VIZ-A", 10L);
        List<Pair<Key,Value>> pairs = List.of(Pair.of(zstdOne, new Value()));

        // serialization util has a different serialization version than the key, decompression required
        EventSerializationUtil util = new EventSerializationUtil();
        util.setSerializationVersion(3);
        assertDecompressionRequired(util, EventSerializationUtil.ZSTD, pairs, true);
    }

    private void assertDecompressionRequired(EventSerializationUtil util, String target, List<Pair<Key,Value>> pairs, boolean expected) {
        util.setCompressionAlgorithm(target);
        assertEquals(expected, util.isDecompressionRequired(pairs));
    }
}
