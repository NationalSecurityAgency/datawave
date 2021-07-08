package datawave.query.index.lookup;

import com.google.common.collect.ImmutableSortedSet;
import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static datawave.query.index.lookup.CreateUidsIterator.COLLAPSE_UIDS;
import static datawave.query.index.lookup.CreateUidsIterator.COLLAPSE_UIDS_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CreateUidsIteratorTest {
    
    @Test
    public void testShardEquals() {
        assertTrue(CreateUidsIterator.sameShard(new Key("row", "cf", "shard_1"), new Key("row2", "cf2", "shard_1\u0000fds")));
        assertFalse(CreateUidsIterator.sameShard(new Key("row", "cf", "shard_1"), new Key("row2", "cf2", "shard_2\u0000fds")));
    }
    
    @Test
    public void testParseDataType() {
        assertEquals("datatype", CreateUidsIterator.parseDataType(new Key("row", "cf", "shard_1\u0000datatype")));
    }
    
    @Test
    public void testLastNull() {
        ArrayByteSequence bs = new ArrayByteSequence("shard_1\u0000datatype".getBytes(Charset.forName("UTF-8")));
        assertEquals("shard_1".length(), CreateUidsIterator.lastNull(bs));
    }
    
    @Test
    public void testMakeRootKey() {
        Key expectedRootKey = new Key("term", "field", "shard");
        Key indexKey = new Key("term", "field", "shard\u0000datatype");
        assertEquals(expectedRootKey, CreateUidsIterator.makeRootKey(indexKey));
    }
    
    /**
     * Ensure that for a known set of data the iterator will correctly seek to each next value.
     *
     * @throws IOException
     */
    @Test
    public void testReseek() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<String> expectedDocs = new LinkedList<>();
        for (int ii = 1; ii < 50; ii++) {
            expectedDocs.add("date_" + ii);
            data.put(new Key("row", "cf", "date_" + ii + "\u0000A"), hasDocs);
        }
        data.put(new Key("row", "cf", "date_2\u0000B"), hasDocs);
        
        // Setup iterator.
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        
        Key startKey = new Key("row", "cf", "date_0");
        Key endKey = new Key("row", "cf", "date_\uffff");
        Range range = new Range(startKey, true, endKey, false);
        
        iterator.seek(range, Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertTrue(iterator.getTopKey().getColumnQualifier().toString().startsWith("date_1"));
        
        Key topKey = iterator.getTopKey();
        String id = topKey.getColumnQualifier().toString();
        expectedDocs.remove(id);
        for (int ii = 2; ii <= 49; ii++) {
            Range seekRange = new Range(iterator.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive());
            iterator.seek(seekRange, Collections.emptySet(), false);
            if (iterator.hasTop()) {
                topKey = iterator.getTopKey();
                id = topKey.getColumnQualifier().toString();
                expectedDocs.remove(id);
            }
        }
        assertEquals("Items remaining " + expectedDocs, 0, expectedDocs.size());
    }
    
    /**
     * Ensure that iterator will work when some Protobuf UIDs are created with the IGNORE flag set to 'true'.
     *
     * @throws IOException
     */
    @Test
    public void testWithIgnore() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expectedDocs = new LinkedList<>();
        
        data.put(new Key("row", "cf", "date_1\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        builder = Uid.List.newBuilder();
        builder.setCOUNT(100);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "date_1\u0000D"), highCount);
        
        // Setup iterator
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        assertEquals(new Key("row", "cf", "date_1"), iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        // One hundred uids from builder plus four documents per key for three keys
        assertEquals(100 + (4 * 3), indexInfo.count());
        assertTrue(indexInfo.uids().isEmpty());
        
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Ensure proper iterator behavior when Protobuf UIDs are build *without* the IGNORE flag set to true.
     */
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
        
        List<IndexMatch> expectedDocs = new LinkedList<>();
        
        data.put(new Key("row", "cf", "date_1\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        // Setup iterator
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), null, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        assertEquals(new Key("row", "cf", "date_1"), iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        // Four documents per key for three keys makes 12
        assertEquals(12, indexInfo.count());
        
        // Assert that all uids in the indexInfo are expected.
        assertEquals(expectedDocs.size(), indexInfo.uids().size());
        assertTrue(expectedDocs.containsAll(indexInfo.uids()));
        
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Ensure correct iterator behavior with COLLAPSE_UIDS option set to "true", should return an {@link IndexInfo} object containing a correct UID count but no
     * stored UIDs.
     */
    @Test
    public void testWithCollapse() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("doc1", "doc2", "doc3", "doc4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expectedDocs = new LinkedList<>();
        
        data.put(new Key("row", "cf", "date_1\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000B"), hasDocs);
        addToExpectedDocs("B", docIds, expectedDocs, null);
        data.put(new Key("row", "cf", "date_1\u0000C"), hasDocs);
        addToExpectedDocs("C", docIds, expectedDocs, null);
        
        builder = Uid.List.newBuilder();
        builder.setCOUNT(5);
        builder.setIGNORE(true);
        Value highCount = new Value(builder.build().toByteArray());
        data.put(new Key("row", "cf", "date_1\u0000D"), highCount);
        
        // Setup iterator
        CreateUidsIterator iterator = new CreateUidsIterator();
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(COLLAPSE_UIDS, "true");
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        assertEquals(new Key("row", "cf", "date_1"), iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        // Five uids from builder plus four documents per key for three keys
        assertEquals(5 + (4 * 3), indexInfo.count());
        assertTrue(indexInfo.uids().isEmpty());
        
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    // For a dataset of TLD uuids, confirm tld uid parsing behavior.
    @Test
    public void testParseTldUidsOption() throws IOException {
        // Setup data for test.
        TreeMap<Key,Value> data = new TreeMap<>();
        List<String> docIds = Arrays.asList("parent.doc.id", "parent.doc.id.child01", "parent.doc.id.child02");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<IndexMatch> expectedDocs = new LinkedList<>();
        
        data.put(new Key("bar", "FOO", "20190314_1\u0000A"), hasDocs);
        addToExpectedDocs("A", docIds, expectedDocs, null);
        
        // Setup iterator
        CreateUidsIterator iterator = new CreateUidsIterator();
        Map<String,String> iteratorOptions = new HashMap<>();
        iteratorOptions.put(CreateUidsIterator.PARSE_TLD_UIDS, "false");
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        assertEquals(new Key("bar", "FOO", "20190314_1"), iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        // 3 uids
        assertEquals(3, indexInfo.count());
        assertFalse(indexInfo.uids().isEmpty());
        Iterator<IndexMatch> matchIter = indexInfo.uids().iterator();
        assertEquals("A\u0000parent.doc.id", matchIter.next().getUid());
        assertEquals("A\u0000parent.doc.id.child01", matchIter.next().getUid());
        assertEquals("A\u0000parent.doc.id.child02", matchIter.next().getUid());
        assertFalse(matchIter.hasNext());
        
        iterator.next();
        assertFalse(iterator.hasTop());
        
        // Now test again but with the PARSE_TLD_UIDS option set to true.
        iterator = new CreateUidsIterator();
        iteratorOptions = new HashMap<>();
        iteratorOptions.put(CreateUidsIterator.PARSE_TLD_UIDS, "true");
        iterator.init(new SortedMapIterator(data), iteratorOptions, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        assertTrue(iterator.hasTop());
        assertEquals(new Key("bar", "FOO", "20190314_1"), iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        // The three uids all share the same root uid, only 1 uid was returned.
        assertEquals(1, indexInfo.count());
        assertFalse(indexInfo.uids().isEmpty());
        
        matchIter = indexInfo.uids().iterator();
        assertEquals("A\u0000parent.doc.id", matchIter.next().getUid());
        assertFalse(matchIter.hasNext());
        
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    static void addToExpectedDocs(String dataType, Iterable<String> docIds, Collection<IndexMatch> expected, JexlNode node) {
        for (String id : docIds)
            expected.add(new IndexMatch(dataType + '\u0000' + id, node));
    }
    
    // Helper method for creating test data quickly
    private static void addToData(TreeMap<Key,Value> data, String shard, String... docsIds) {
        addToData(data, false, shard, docsIds);
    }
    
    // Helper method for creating test data quickly
    private static void addToData(TreeMap<Key,Value> data, boolean ignore, String shard, String... docsIds) {
        // DataType is FOO
        String columnQualifier = shard + '\u0000' + "FOO";
        addKeyValueToData(data, ignore, columnQualifier, Arrays.asList(docsIds));
    }
    
    // Helper method for creating test data quickly
    private static void addKeyValueToData(TreeMap<Key,Value> data, boolean ignore, String cq, List<String> docIds) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(docIds.size());
        if (ignore) {
            builder.setIGNORE(true);
        } else {
            builder.addAllUID(docIds);
            builder.setIGNORE(false);
        }
        Value docs = new Value(builder.build().toByteArray());
        
        Key key = new Key("row", "cf", cq);
        data.put(key, docs);
    }
    
    // Helper method for creating expected data quickly
    static void addToExpected(Collection<IndexMatch> expected, String dataType, String... docIds) {
        addToExpected(expected, dataType, null, docIds);
    }
    
    // Helper method for creating expected data quickly
    static void addToExpected(Collection<IndexMatch> expected, String dataType, JexlNode node, String... docIds) {
        for (String id : docIds)
            expected.add(new IndexMatch(dataType + '\u0000' + id, node));
    }
    
    /**
     * Iterator with empty options should bring back all document specific ranges.
     *
     * <pre>
     * COLLAPSE_UIDS = false
     * COLLAPSE_UIDS_THRESHOLD = Integer.MAX_VALUE
     * </pre>
     */
    @Test
    public void testEmptyOptions() throws IOException {
        // Setup data for test
        TreeMap<Key,Value> data = new TreeMap<>();
        addToData(data, "20190314_0", "doc0");
        addToData(data, "20190314_1", "doc1");
        // 20190314_2 should collapse due to IGNORE = TRUE
        addToData(data, true, "20190314_2", "doc2");
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // Build expected data
        List<IndexMatch> expectedDocs = new LinkedList<>();
        addToExpected(expectedDocs, "FOO", "doc0");
        addToExpected(expectedDocs, "FOO", "doc1");
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("row", "cf", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_0
        assertEquals(1L, indexInfo.count());
        assertEquals(1L, indexInfo.uids().size());
        assertEquals(expectedDocs.get(0).getUid(), indexInfo.uids().iterator().next().getUid());
        
        // Build index info for shard 20190314_1
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_1");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_1
        assertEquals(1L, indexInfo.count());
        assertEquals(1L, indexInfo.uids().size());
        assertEquals(expectedDocs.get(1).getUid(), indexInfo.uids().iterator().next().getUid());
        
        // Build index info for shard 20190314_2
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_2");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_2
        assertEquals(1L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Iterator should collapse all document ranges into shard ranges.
     *
     * <pre>
     * COLLAPSE_UIDS = true
     * COLLAPSE_UIDS_THRESHOLD = Integer.MAX_VALUE
     * </pre>
     */
    @Test
    public void testCollapseDocRanges() throws IOException {
        // Setup data for test
        TreeMap<Key,Value> data = new TreeMap<>();
        addToData(data, "20190314_0", "doc0", "doc1", "doc2");
        addToData(data, "20190314_1", "doc4");
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS, "true");
        // Setting this option to default value. Should not affect test behavior.
        options.put(COLLAPSE_UIDS_THRESHOLD, "" + Integer.MAX_VALUE);
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // No expected data as all uids are collapsed, only counts are returned.
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("row", "cf", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_0
        assertEquals(3L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Build index info for shard 20190314_1
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_1");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_1
        assertEquals(1L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Iterator should only collapse document ranges into shard ranges for shard ranges with more document ids than the defined threshold.
     *
     * <pre>
     * COLLAPSE_UIDS = false
     * COLLAPSE_UIDS_THRESHOLD = 2
     * </pre>
     */
    @Test
    public void testCollapseDocRangesByThreshold() throws IOException {
        // Setup data for test
        TreeMap<Key,Value> data = new TreeMap<>();
        addToData(data, "20190314_0", "doc0", "doc1", "doc2");
        addToData(data, "20190314_1", "doc4", "doc5");
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS_THRESHOLD, "2");
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // Build expected data
        List<IndexMatch> expectedDocs = new LinkedList<>();
        addToExpected(expectedDocs, "FOO", "doc4");
        addToExpected(expectedDocs, "FOO", "doc5");
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("row", "cf", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_0
        assertEquals(3L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Build index info for shard 20190314_1
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_1");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_1
        assertEquals(2L, indexInfo.count());
        assertEquals(2L, indexInfo.uids().size());
        assertEquals(expectedDocs.get(0).getUid(), indexInfo.uids().first().getUid());
        assertEquals(expectedDocs.get(1).getUid(), indexInfo.uids().last().getUid());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Iterator should only collapse document ranges into shard ranges for shard ranges with more document ids than the defined threshold.
     *
     * This test asserts the case when the Uids have the IGNORE flag set to true. In this case the actual document id is not present and counts may not be
     * correct. Asserts will have no document ids present.
     *
     * NOTE: When the IGNORE flag is set to TRUE, it is a hint to ignore building document specific ranges, instead build shard ranges.
     *
     * <pre>
     * COLLAPSE_UIDS = false
     * COLLAPSE_UIDS_THRESHOLD = 2
     * </pre>
     */
    @Test
    public void testCollapseDocRangesByThresholdWithIgnore() throws IOException {
        // Setup data for test
        TreeMap<Key,Value> data = new TreeMap<>();
        // 20190314_0 collapses due to threshold
        addToData(data, true, "20190314_0", "doc0", "doc1", "doc2");
        // 20190314_1 collapses due to IGNORE = TRUE
        addToData(data, true, "20190314_1", "doc4", "doc5");
        // 20190314_2 does not collapse.
        addToData(data, false, "20190314_2", "doc6", "doc7");
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS_THRESHOLD, "2");
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("row", "cf", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_0
        assertEquals(3L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Build index info for shard 20190314_1
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_1");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_1
        assertEquals(2L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Build index info for shard 20190314_2
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_2");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_2
        assertEquals(2L, indexInfo.count());
        assertEquals(2L, indexInfo.uids().size());
        
        // Build expected data
        List<IndexMatch> expectedDocs = new LinkedList<>();
        addToExpected(expectedDocs, "FOO", "doc6");
        addToExpected(expectedDocs, "FOO", "doc7");
        assertEquals(ImmutableSortedSet.copyOf(expectedDocs), indexInfo.uids());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    /**
     * Iterator should collapse document ranges into shard ranges using the threshold, respecting the order of precedence. The data in shard 20190314_1 should
     * return a document specific range as in the above test.
     *
     * <pre>
     * COLLAPSE_UIDS = true
     * COLLAPSE_UIDS_THRESHOLD = 2
     * </pre>
     */
    @Test
    public void testCollapseByOrderOfPrecedence() throws IOException {
        // Setup data for test
        TreeMap<Key,Value> data = new TreeMap<>();
        addToData(data, "20190314_0", "doc0", "doc1", "doc2");
        addToData(data, "20190314_1", "doc4");
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS, "true");
        options.put(COLLAPSE_UIDS_THRESHOLD, "2");
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // Build expected data
        List<IndexMatch> expectedDocs = new LinkedList<>();
        addToExpected(expectedDocs, "FOO", "doc4");
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("row", "cf", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_0
        assertEquals(3L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Build index info for shard 20190314_1
        iterator.next();
        assertTrue(iterator.hasTop());
        expectedTopKey = new Key("row", "cf", "20190314_1");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        
        // Assert index info is correct for shard 20190314_1
        assertEquals(1L, indexInfo.count());
        assertEquals(1L, indexInfo.uids().size());
        assertEquals(expectedDocs.get(0).getUid(), indexInfo.uids().iterator().next().getUid());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testRespectIgnoreFlag() throws IOException {
        // Setup data for test
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(42);
        builder.addUID("uid");
        builder.setIGNORE(true);
        
        Value docs = new Value(builder.build().toByteArray());
        Key key = new Key("bar", "FOO", "20190314_0\u0000datatype");
        
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(key, docs);
        
        // Setup empty options
        Map<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS, "false");
        options.put(COLLAPSE_UIDS_THRESHOLD, String.valueOf(Integer.MAX_VALUE)); // Use default value for collapse threshold
        
        // Create iterator & seek
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(new SortedMapIterator(data), options, null);
        iterator.seek(new Range(), Collections.emptySet(), false);
        
        // Build index info for shard 20190314_0
        assertTrue(iterator.hasTop());
        Key expectedTopKey = new Key("bar", "FOO", "20190314_0");
        assertEquals(expectedTopKey, iterator.getTopKey());
        
        // Assert index info is correct for shard 20190314_0
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.readFields(new DataInputStream(new ByteArrayInputStream(iterator.getTopValue().get())));
        assertEquals(42L, indexInfo.count());
        assertEquals(0L, indexInfo.uids().size());
        
        // Assert absence of unexpected results
        iterator.next();
        assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testValidateAndDescribeOptions() {
        HashMap<String,String> options = new HashMap<>();
        CreateUidsIterator iterator = new CreateUidsIterator();
        assertTrue(iterator.validateOptions(options));
        
        OptionDescriber.IteratorOptions iterOpts = iterator.describeOptions();
        assertEquals("", iterOpts.getName());
        assertEquals("", iterOpts.getDescription());
        assertTrue(iterOpts.getNamedOptions().isEmpty());
        assertTrue(iterOpts.getUnnamedOptionDescriptions().isEmpty());
    }
    
    /**
     * Given an improper configuration, throw an exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadArgument_CollapseDocRangesThreshold() throws IOException {
        // Set bad options to cause an exception
        HashMap<String,String> options = new HashMap<>();
        options.put(COLLAPSE_UIDS_THRESHOLD, "false");
        CreateUidsIterator iterator = new CreateUidsIterator();
        iterator.init(null, options, null);
    }
}
