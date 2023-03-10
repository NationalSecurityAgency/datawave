package datawave.query.index.lookup;

import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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
        iteratorOptions.put(CreateUidsIterator.COLLAPSE_UIDS, "true");
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
}
