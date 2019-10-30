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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD2", "VALUE2", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Union.
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "VALUE");
        JexlNode eqNode2 = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        
        ScannerStream leftStream = ScannerStream.withData(leftIter, eqNode);
        ScannerStream rightStream = ScannerStream.withData(rightIter, eqNode2);
        
        Union union = new Union(Lists.newArrayList(leftStream, rightStream));
        
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> peekedTuple = union.peek();
        Tuple2<String,IndexInfo> nextedTuple = union.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        List<IndexMatch> expectedDocs = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3", "doc4");
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
}
