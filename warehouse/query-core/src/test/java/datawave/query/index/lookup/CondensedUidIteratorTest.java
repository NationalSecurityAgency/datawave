package datawave.query.index.lookup;

import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * All tests shard by date, and the best day is pie day.
 */
public class CondensedUidIteratorTest {
    
    @Test
    public void testWithoutIgnore() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_03\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(100);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "s\u0000D"), highCount);
        
        // Setup CondensedUidIterator options
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CondensedUidIterator.SHARDS_TO_EVALUATE, "10");
        
        // Setup CondensedUidIterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info.
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals("20190314", indexInfo.getDay());
        assertEquals(4, indexInfo.getShard("20190314_01").uids().size());
        assertEquals("A\u0000doc1", indexInfo.getShard("20190314_01").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_02").uids().size());
        assertEquals("B\u0000doc1", indexInfo.getShard("20190314_02").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_03").uids().size());
        assertEquals("C\u0000doc1", indexInfo.getShard("20190314_03").uids().iterator().next().getUid());
        
        // Iterate across all three shards and assert no more values left to read.
        iterator.next();
        assertTrue(iterator.hasTop());
        iterator.next();
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testWithoutIgnoreMultipleDays() throws IOException {
        // Create sample data for day 1
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Collections.singletonList("doc1");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_03\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Create sample data for day 2
        docIds = Collections.singletonList("doc2");
        builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        hasDocs = new Value(builder.build().toByteArray());
        
        // Add sample data for day 2 to expected data
        data.put(new Key("row", "cf", "20190315_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190315_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190315_03\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(100);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "s\u0000D"), highCount);
        
        // Setup CondensedUidIterator options
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CondensedUidIterator.SHARDS_TO_EVALUATE, "10");
        
        // Setup CondensedUidIterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info for day 1
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals("20190314", indexInfo.getDay());
        assertEquals(1, indexInfo.getShard("20190314_01").uids().size());
        assertEquals("A\u0000doc1", indexInfo.getShard("20190314_01").uids().iterator().next().getUid());
        assertEquals(1, indexInfo.getShard("20190314_02").uids().size());
        assertEquals("B\u0000doc1", indexInfo.getShard("20190314_02").uids().iterator().next().getUid());
        assertEquals(1, indexInfo.getShard("20190314_03").uids().size());
        assertEquals("C\u0000doc1", indexInfo.getShard("20190314_03").uids().iterator().next().getUid());
        
        // Assert proper condensed index info for day 2
        iterator.next();
        indexInfo = new CondensedIndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals("20190315", indexInfo.getDay());
        assertEquals(1, indexInfo.getShard("20190315_01").uids().size());
        assertEquals("A\u0000doc2", indexInfo.getShard("20190315_01").uids().iterator().next().getUid());
        assertEquals(1, indexInfo.getShard("20190315_02").uids().size());
        assertEquals("B\u0000doc2", indexInfo.getShard("20190315_02").uids().iterator().next().getUid());
        assertEquals(1, indexInfo.getShard("20190315_03").uids().size());
        assertEquals("C\u0000doc2", indexInfo.getShard("20190315_03").uids().iterator().next().getUid());
        assertTrue(iterator.hasTop());
    }
    
    @Test
    public void testWithIgnoreLateWholeDay() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(30000);
        builder.setIGNORE(true);
        hasDocs = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "20190314_04\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Setup iterator options
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CondensedUidIterator.SHARDS_TO_EVALUATE, "2");
        iteratorOptions.put(CondensedUidIterator.MAX_IDS, "1");
        
        // Setup iterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals("20190314", indexInfo.getDay());
        assertTrue(indexInfo.isDay());
        
        // Assert that no more elements exist
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testWithIgnoreLateShards() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(10);
        builder.setIGNORE(true);
        hasDocs = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "20190314_04\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Setup iterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals("20190314", indexInfo.getDay());
        assertFalse(indexInfo.isDay());
        
        assertEquals(4, indexInfo.getShard("20190314_01").uids().size());
        assertEquals("A\u0000doc1", indexInfo.getShard("20190314_01").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_02").uids().size());
        assertEquals("B\u0000doc1", indexInfo.getShard("20190314_02").uids().iterator().next().getUid());
        
        // Assert that no more elements exist
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * In the event of a teardown Accumulo will hand back the last key RETURNED to the seek() along with isInclusive=false
     */
    @Test
    public void testTearDown() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_03\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(100);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "s\u0000D"), highCount);
        
        // Setup CondensedUidIterator options
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CondensedUidIterator.SHARDS_TO_EVALUATE, "10");
        
        // Setup CondensedUidIterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        // For this test hang onto the top key
        Key lastKey = iterator.getTopKey();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        assertEquals("20190314", indexInfo.getDay());
        assertEquals(4, indexInfo.getShard("20190314_01").uids().size());
        assertEquals("A\u0000doc1", indexInfo.getShard("20190314_01").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_02").uids().size());
        assertEquals("B\u0000doc1", indexInfo.getShard("20190314_02").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_03").uids().size());
        assertEquals("C\u0000doc1", indexInfo.getShard("20190314_03").uids().iterator().next().getUid());
        
        // Create new iterator and seek based on the last key
        iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        Range newSeekRange = new Range(lastKey, false, new Key("z"), true);
        iterator.seek(newSeekRange, Collections.emptySet(), false);
        
        assertTrue(iterator.hasTop());
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * In the event of a teardown Accumulo will hand back the last key RETURNED to the seek() along with isInclusive=false
     */
    @Test
    public void testTearDownBeyondEnd() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        // Create small sample set of known data.
        List<IndexMatch> expectedDocs = new LinkedList<>();
        data.put(new Key("row", "cf", "20190314_01\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_02\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "20190314_03\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Add more UIDs
        builder = Uid.List.newBuilder();
        builder.setCOUNT(100);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "s\u0000D"), highCount);
        
        // Setup CondensedUidIterator options
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CondensedUidIterator.SHARDS_TO_EVALUATE, "10");
        
        // Setup CondensedUidIterator
        CondensedUidIterator iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        // Assert proper condensed index info
        CondensedIndexInfo indexInfo = new CondensedIndexInfo();
        // For this test hang onto the top key
        Key lastKey = iterator.getTopKey();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        assertEquals("20190314", indexInfo.getDay());
        assertEquals(4, indexInfo.getShard("20190314_01").uids().size());
        assertEquals("A\u0000doc1", indexInfo.getShard("20190314_01").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_02").uids().size());
        assertEquals("B\u0000doc1", indexInfo.getShard("20190314_02").uids().iterator().next().getUid());
        assertEquals(4, indexInfo.getShard("20190314_03").uids().size());
        assertEquals("C\u0000doc1", indexInfo.getShard("20190314_03").uids().iterator().next().getUid());
        
        // Create new iterator and seek based on the last key
        iterator = new CondensedUidIterator();
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        Range newSeekRange = new Range(lastKey, false, lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), true);
        iterator.seek(newSeekRange, Collections.emptySet(), false);
        
        assertFalse(iterator.hasTop());
    }
    
    static void addToExpectedDocs(String dataType, Iterable<String> docIds, Collection<IndexMatch> expected, JexlNode node) {
        for (String id : docIds)
            expected.add(new IndexMatch(dataType + '\u0000' + id, node));
    }
}
