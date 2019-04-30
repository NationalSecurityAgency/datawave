package datawave.query.index.lookup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntersectionTest {
    
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
     * Intersection of two terms hitting on the same shard.
     */
    @Test
    public void testIntersection_SameShardStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        // Assert the Intersection
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Intersection with one term being a high cardinality hit.
     */
    @Test
    public void testIntersection_OneTermIsHighCardinality() {
        // Build a peeking iterator for a left side term. High cardinality means only counts, no document ids.
        IndexInfo left = new IndexInfo(100L);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        // Assert the Intersection
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3", "doc4");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Cannot intersect two disjoint shards.
     */
    @Test
    public void testIntersection_DifferentShardStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_1", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Intersection of day range with a shard range within the day range is possible.
     */
    @Test
    public void testIntersection_ShardAndDayStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    @Test
    public void testIntersection_EmptyExceededValueThreshold() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("THIS_FIELD == 20");
        ScannerStream scannerStream = ScannerStream.exceededValueThreshold(Collections.emptyIterator(), script);
        List<? extends IndexStream> iterable = Collections.singletonList(scannerStream);
        
        Intersection intersection = new Intersection(iterable, null);
        assertEquals(IndexStream.StreamContext.ABSENT, intersection.context());
    }
}
