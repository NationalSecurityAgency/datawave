package datawave.query.index.lookup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Some basic tests of the {@link Union} class.
 */
public class UnionTest {
    
    // Helper method to generate index matches (document id - field, value)
    private List<IndexMatch> buildIndexMatches(String field, String value, String... docIds) {
        List<IndexMatch> matches = new ArrayList<>(docIds.length);
        for (String docId : docIds) {
            matches.add(buildIndexMatch(field, value, docId));
        }
        return matches;
    }
    
    // Helper method to generate index matches (document id - field, value)
    private IndexMatch buildIndexMatch(String field, String value, String docId) {
        JexlNode eqNode = JexlNodeFactory.buildEQNode(field, value);
        return new IndexMatch(docId, eqNode);
    }
    
    // Helper method to generate expected index matches (document id only)
    private Set<IndexMatch> buildExpectedIndexMatches(String... docIds) {
        Set<IndexMatch> expected = new HashSet<>(docIds.length);
        for (String docId : docIds) {
            expected.add(new IndexMatch(docId));
        }
        return expected;
    }
    
    /**
     * This test builds two IndexStreams for a query containing two terms, left and right.
     * <p>
     * Left term hits on documents (1,2,3) Right term hits on documents (2,3,4) The union of left and right is (1,2,3,4)
     */
    @Test
    public void testUnionOfTwoDocumentStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Union.
        Union union = new Union(Lists.newArrayList(ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE")),
                        ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"))));
        
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> peekedTuple = union.peek();
        Tuple2<String,IndexInfo> nextedTuple = union.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc1", "doc2", "doc3", "doc4");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(union.hasNext());
    }
    
    /**
     * Similar to the above test except the left term is a day range.
     * <p>
     * The union of a shard range and a day range is a day range.
     */
    @Test
    public void testUnionOfShardAndDayStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Union.
        Union union = new Union(Lists.newArrayList(ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE")),
                        ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"))));
        
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> peekedTuple = union.peek();
        Tuple2<String,IndexInfo> nextedTuple = union.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc1", "doc2", "doc3", "doc4");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(union.hasNext());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekBeforeStreamStart() {
        // Seek to a day range before the IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190202"));
        assertEquals("20190301_0", union.next().first());
        
        // Seek to a day-underscore range before the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190202_"));
        assertEquals("20190301_0", union.next().first());
        
        // Seek to a shard range before the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190202_0"));
        assertEquals("20190301_0", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekToStreamStart() {
        // Seek to a day range at the start of the IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190301"));
        assertEquals("20190301_0", union.next().first());
        
        // Seek to a day-underscore range at the start of the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190301_"));
        assertEquals("20190301_0", union.next().first());
        
        // Seek to a shard range before at the start of the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190301_0"));
        assertEquals("20190301_0", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekToMiddleOfStream() {
        // Seek to a day range in the middle of the IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190303_0", union.seek("20190303"));
        assertEquals("20190303_0", union.next().first());
        
        // Seek to a day-underscore range in the middle of the IndexStream
        union = buildUnionOfShards();
        assertEquals("20190303_0", union.seek("20190303_"));
        assertEquals("20190303_0", union.next().first());
        
        // Seek to a shard range in the middle of the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190303_3", union.seek("20190303_3"));
        assertEquals("20190303_3", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekToNonExistentMiddleOfStream() {
        // Seek to a non-existent day range in the middle of the IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190307_0", union.seek("20190305"));
        assertEquals("20190307_0", union.next().first());
        
        // Seek to a non-existent day-underscore range to the middle of the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190307_0", union.seek("20190305_"));
        assertEquals("20190307_0", union.next().first());
        
        // Seek to a non-existent shard range to the middle of of the IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190307_0", union.seek("20190305_0"));
        assertEquals("20190307_0", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekToEndOfStream() {
        // Seek to the last day range in this IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190309_0", union.seek("20190309"));
        assertEquals("20190309_0", union.next().first());
        
        // Seek to the last day-underscore range in this IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190309_0", union.seek("20190309_"));
        assertEquals("20190309_0", union.next().first());
        
        // Seek to the last shard range in this IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertEquals("20190309_9", union.seek("20190309_9"));
        assertEquals("20190309_9", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfShards_seekBeyondEndOfStream() {
        // Seek to a day range beyond the end of this IndexStream
        Union union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190310"));
        assertFalse(union.hasNext());
        assertNull(union.next());
        
        // Seek to a day-underscore range beyond the end of this IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190310_"));
        assertFalse(union.hasNext());
        assertNull(union.next());
        
        // Seek to a shard range beyond the end of this IndexStream
        union = buildUnionOfShards();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190309_99"));
        assertFalse(union.hasNext());
        assertNull(union.next());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekBeforeStreamStart() {
        // Seek to a day range before the IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301", union.seek("20190202"));
        assertEquals("20190301", union.next().first());
        
        // Seek to a day-underscore range before the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301", union.seek("20190202_"));
        assertEquals("20190301", union.next().first());
        
        // Seek to a shard range before the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301", union.seek("20190202_0"));
        assertEquals("20190301", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekToStreamStart() {
        // Seek to a day range at the start of the IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301", union.seek("20190301"));
        assertEquals("20190301", union.next().first());
        
        // Seek to a day-underscore range at the start of the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301", union.seek("20190301_"));
        assertEquals("20190301", union.next().first());
        
        // Seek to a shard range before at the start of the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190301_0", union.seek("20190301_0"));
        assertEquals("20190301_0", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekToMiddleOfStream() {
        // Seek to a day range in the middle of the IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190303", union.seek("20190303"));
        assertEquals("20190303", union.next().first());
        
        // Seek to a day-underscore range in the middle of the IndexStream
        union = buildUnionOfDays();
        assertEquals("20190303", union.seek("20190303_"));
        assertEquals("20190303", union.next().first());
        
        // Seek to a shard range in the middle of the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190303", union.seek("20190303_3"));
        assertEquals("20190303", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekToNonExistentMiddleOfStream() {
        // Seek to a non-existent day range in the middle of the IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190307", union.seek("20190305"));
        assertEquals("20190307", union.next().first());
        
        // Seek to a non-existent day-underscore range to the middle of the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190307", union.seek("20190305_"));
        assertEquals("20190307", union.next().first());
        
        // Seek to a non-existent shard range to the middle of of the IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190307", union.seek("20190305_0"));
        assertEquals("20190307", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekToEndOfStream() {
        // Seek to the last day range in this IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190309", union.seek("20190309"));
        assertEquals("20190309", union.next().first());
        
        // Seek to the last day-underscore range in this IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190309", union.seek("20190309_"));
        assertEquals("20190309", union.next().first());
        
        // Seek to the last shard range in this IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertEquals("20190309", union.seek("20190309_9"));
        assertEquals("20190309", union.next().first());
    }
    
    // A && B
    @Test
    public void testUnionOfDays_seekBeyondEndOfStream() {
        // Seek to a day range beyond the end of this IndexStream
        Union union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190310"));
        assertFalse(union.hasNext());
        assertNull(union.next());
        
        // Seek to a day-underscore range beyond the end of this IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190310_"));
        assertFalse(union.hasNext());
        assertNull(union.next());
        
        // Seek to a shard range beyond the end of this IndexStream
        union = buildUnionOfDays();
        assertTrue(union.hasNext());
        assertNull(union.seek("20190310_0"));
        assertFalse(union.hasNext());
        assertNull(union.next());
    }
    
    private Union buildUnionOfShards() {
        List<String> ignoredDays = Lists.newArrayList("20190304", "20190305", "20190306");
        SortedSet<String> shards = buildShards(ignoredDays);
        
        ScannerStream s1 = buildFullScannerStream(shards, "A", "1");
        ScannerStream s2 = buildFullScannerStream(shards, "B", "2");
        
        return new Union(Arrays.asList(s1, s2));
    }
    
    private Union buildUnionOfDays() {
        List<String> ignoredDays = Lists.newArrayList("20190304", "20190305", "20190306");
        SortedSet<String> shards = buildDays(ignoredDays);
        
        ScannerStream s1 = buildFullScannerStream(shards, "A", "1");
        ScannerStream s2 = buildFullScannerStream(shards, "B", "2");
        
        return new Union(Arrays.asList(s1, s2));
    }
    
    // Build a set of shards for the first 9 days in march, with the option to ignore days.
    private SortedSet<String> buildShards(List<String> ignoredDays) {
        SortedSet<String> shards = new TreeSet<>();
        for (int ii = 1; ii < 10; ii++) {
            String day = "2019030" + ii;
            if (ignoredDays.contains(day)) {
                continue;
            }
            for (int jj = 0; jj < 20; jj++) {
                shards.add(day + "_" + jj);
            }
        }
        return shards;
    }
    
    // Build a set of day shards for the first 9 days in march, with the option to ignore days.
    private SortedSet<String> buildDays(List<String> ignoredDays) {
        SortedSet<String> shards = new TreeSet<>();
        for (int ii = 1; ii < 10; ii++) {
            String day = "2019030" + ii;
            if (ignoredDays.contains(day)) {
                continue;
            }
            shards.add(day);
        }
        return shards;
    }
    
    // Build a ScannerStream specifically for testing the ability to seek through the stream.
    private ScannerStream buildFullScannerStream(SortedSet<String> shards, String field, String value) {
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        
        List<Tuple2<String,IndexInfo>> elements = new ArrayList<>();
        for (String shard : shards) {
            IndexInfo info = new IndexInfo(-1);
            info.applyNode(node);
            elements.add(new Tuple2<>(shard, info));
        }
        
        return ScannerStream.variable(elements.iterator(), node);
    }
}
