package datawave.query.index.lookup;

import com.google.common.collect.ImmutableSortedSet;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test basic functionality of the {@link IndexInfo} class.
 */
public class IndexInfoTest {
    
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
     * Intersection of query terms when both terms have document ids.
     */
    @Test
    public void testIntersection_BothTermsHaveDocIds() {
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        
        IndexInfo merged = left.intersect(right);
        
        // The intersection of left and right should be a set of two document ids
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3");
        
        assertEquals(2, merged.uids().size());
        assertEquals(expectedDocs, merged.uids());
    }
    
    /**
     * Intersection of query terms when only one term has document ids.
     */
    @Test
    public void testIntersection_OnlyOneTermHasDocIds() {
        IndexInfo left = new IndexInfo(20L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        
        IndexInfo merged = left.intersect(right);
        
        // The intersection of left and right should be a set of 3 document ids
        List<IndexMatch> expectedDocs = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedDocs);
        
        assertEquals(3, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
    }
    
    /**
     * Intersection of query terms when neither term has document ids. The intersection of {50,90} is {50}
     */
    @Test
    public void testIntersection_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo merged = left.intersect(right);
        assertEquals(50L, merged.count());
        assertTrue(merged.uids().isEmpty());
    }
    
    /**
     * Intersection of query terms when one term is infinite. The intersection of {inf, 3} is {3}
     */
    @Test
    public void testIntersection_InfiniteTerm() {
        IndexInfo left = new IndexInfo(-1L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);
        
        List<IndexMatch> expectedMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo expectedMerged = new IndexInfo(expectedMatches);
        assertEquals(expectedMerged, left.intersect(right));
        assertEquals(expectedMerged, right.intersect(left));
    }
    
    /**
     * Union of query terms when both terms have document ids.
     */
    @Test
    public void testUnion_BothTermsHaveDocIds() {
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        
        IndexInfo merged = left.union(right);
        
        // The union of left and right should be a set of four document ids
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc1", "doc2", "doc3", "doc4");
        
        assertEquals(4, merged.uids().size());
        assertEquals(expectedDocs, merged.uids());
    }
    
    /**
     * Union of query terms when one term is a delayed predicate
     */
    @Test
    public void testUnion_OneTermIsDelayedPredicate() {
        ASTDelayedPredicate delayedPredicate = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo left = new IndexInfo(50);
        left.applyNode(delayedPredicate);
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo merged = right.union(left);
        assertEquals(0, merged.uids().size());
        assertEquals("Count should be 53 but is " + merged.count(), 53, merged.count());
        String expectedQuery = "(((ASTDelayedPredicate = true) && (FIELD == 'VALUE')))";
        String actualQuery = JexlStringBuildingVisitor.buildQuery(merged.getNode());
        assertEquals(expectedQuery, actualQuery);
    }
    
    /**
     * Union of query terms when neither term has document ids. The union of {50, 90} is {140}.
     */
    @Test
    public void testUnion_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        IndexInfo merged = left.union(right);
        assertEquals(140L, merged.count());
        assertTrue(merged.uids().isEmpty());
    }
    
    /**
     * Union of query terms when one term is infinite. The union of {inf, 3} is {inf}
     */
    @Test
    public void testUnion_InfiniteTerm() {
        IndexInfo left = new IndexInfo(-1L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);
        
        IndexInfo expectedMerged = new IndexInfo(-1L);
        assertEquals(expectedMerged, left.union(right));
        assertEquals(expectedMerged, right.union(left));
    }
}
