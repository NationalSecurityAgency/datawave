package datawave.query.index.lookup;

import com.google.common.collect.ImmutableSortedSet;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.language.parser.jexl.JexlNodeSet;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test basic functionality of the {@link IndexInfo} class.
 */
public class IndexInfoTest {
    
    // Helper method to generate expected index matches (document id only)
    private Set<IndexMatch> buildExpectedIndexMatches(String field, String value, String... docIds) {
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        Set<IndexMatch> expected = new HashSet<>(docIds.length);
        for (String docId : docIds) {
            expected.add(new IndexMatch(docId, node));
        }
        return expected;
    }
    
    /**
     * Intersection of query terms when both terms have document ids.
     */
    @Test
    public void testIntersection_BothTermsHaveDocIds() {
        IndexInfo left = new IndexInfo(Arrays.asList("doc1", "doc2", "doc3"));
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(Arrays.asList("doc2", "doc3", "doc4"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo merged = left.intersect(right);
        
        // The intersection of left and right should be a set of two document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc2", "doc3");
        
        assertEquals(2, merged.uids().size());
        assertEquals(expectedMatches, merged.uids());
    }
    
    /**
     * Intersection of query terms when only one term has document ids.
     */
    @Test
    public void testIntersection_OnlyOneTermHasDocIds() {
        IndexInfo left = new IndexInfo(20L);
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(Arrays.asList("doc2", "doc3", "doc4"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo merged = left.intersect(right);
        
        // The intersection of left and right should be a set of 3 document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc2", "doc3", "doc4");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedMatches);
        
        assertEquals(3, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
    }
    
    @Test
    public void testIntersection_NestedOrTermHasDocIdsInfiniteToSmall() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("A1", "11"));
        children.add(JexlNodeFactory.buildEQNode("A2", "12"));
        children.add(JexlNodeFactory.buildEQNode("A3", "13"));
        
        left.applyNode(JexlNodeFactory.createOrNode(children));
        
        IndexInfo right = new IndexInfo(Collections.singleton("doc1"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("A", "1"));
        
        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);
        
        IndexInfo merged = left.intersect(right);
        
        // The intersection of left and right should be a set of 1 document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedMatches);
        
        assertEquals(1, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
        
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode()),
                        new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testIntersection_NestedOrTermHasDocIdsSmallToInfinite() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("A1", "11"));
        children.add(JexlNodeFactory.buildEQNode("A2", "12"));
        children.add(JexlNodeFactory.buildEQNode("A3", "13"));
        
        left.applyNode(JexlNodeFactory.createOrNode(children));
        
        IndexInfo right = new IndexInfo(Collections.singleton("doc1"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("A", "1"));
        
        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);
        
        IndexInfo merged = right.intersect(left);
        
        // The intersection of left and right should be a set of 1 document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedMatches);
        
        assertEquals(1, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode()),
                        new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testIntersection_NestedOrTermHasDocIdsInfiniteToSmallWithDelayed() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("A1", "11"));
        children.add(JexlNodeFactory.buildEQNode("A2", "12"));
        children.add(JexlNodeFactory.buildEQNode("A3", "13"));
        
        left.applyNode(JexlNodeFactory.createOrNode(children));
        
        IndexInfo right = new IndexInfo(Collections.singleton("doc1"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        JexlNode delayed = JexlNodeFactory.buildEQNode("DELAYED_A", "DELAYED_1");
        
        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("A", "1"));
        andChildren.add(delayed);
        
        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);
        
        IndexInfo merged = left.intersect(right, Arrays.asList(delayed), left);
        
        // The intersection of left and right should be a set of 1 document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedMatches);
        
        assertEquals(1, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode()),
                        new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testIntersection_NestedOrTermHasDocIdsSmallToInfiniteWithDelay() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("A1", "11"));
        children.add(JexlNodeFactory.buildEQNode("A2", "12"));
        children.add(JexlNodeFactory.buildEQNode("A3", "13"));
        
        left.applyNode(JexlNodeFactory.createOrNode(children));
        
        IndexInfo right = new IndexInfo(Collections.singleton("doc1"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        JexlNode delayed = JexlNodeFactory.buildEQNode("DELAYED_A", "DELAYED_1");
        
        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("A", "1"));
        andChildren.add(delayed);
        
        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);
        
        IndexInfo merged = right.intersect(left, Arrays.asList(delayed), right);
        
        // The intersection of left and right should be a set of 1 document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedMatches);
        
        assertEquals(1, merged.uids().size());
        assertEquals(expectedSorted, merged.uids());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode()),
                        new TreeEqualityVisitor.Reason()));
    }
    
    /**
     * Intersection of query terms when neither term has document ids. The intersection of {50,90} is {50}
     */
    @Test
    public void testIntersection_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
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
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(Arrays.asList("doc1", "doc2", "doc3"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1", "doc2", "doc3");
        IndexInfo expectedMerged = new IndexInfo(expectedMatches);
        assertEquals(expectedMerged, left.intersect(right));
        assertEquals(expectedMerged, right.intersect(left));
    }
    
    /**
     * Union of query terms when both terms have document ids.
     */
    @Test
    public void testUnion_BothTermsHaveDocIds() {
        IndexInfo left = new IndexInfo(Arrays.asList("doc1", "doc2", "doc3"));
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(Arrays.asList("doc2", "doc3", "doc4"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo merged = new IndexInfo();
        merged = merged.unionAll(Arrays.asList(left, right), null, false);
        
        // The union of left and right should be a set of four document ids
        Set<IndexMatch> expectedMatches = buildExpectedIndexMatches("A", "1", "doc1", "doc2", "doc3", "doc4");
        
        assertEquals(4, merged.uids().size());
        assertEquals(expectedMatches, merged.uids());
    }
    
    /**
     * Union of query terms when one term is a delayed predicate
     */
    @Test
    public void testUnion_OneTermIsDelayedPredicate() {
        ASTDelayedPredicate delayedPredicate = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo delayed = new IndexInfo(50);
        delayed.applyNode(delayedPredicate);
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(delayedPredicate);
        
        IndexInfo left = new IndexInfo(Arrays.asList("doc1", "doc2", "doc3"));
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo merged = new IndexInfo();
        merged = merged.unionAll(Arrays.asList(left), nodeSet, false);
        
        assertEquals(3, merged.uids().size());
        assertEquals("Count should be 3 but is " + merged.count(), 3, merged.count());
        String expectedQuery = "((ASTDelayedPredicate = true) && (A == '1'))";
        String actualQuery = JexlStringBuildingVisitor.buildQuery(merged.getNode());
        assertEquals(expectedQuery, actualQuery);
    }
    
    /**
     * Union of query terms when neither term has document ids. The union of {50, 90} is {140}.
     */
    @Test
    public void testUnion_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo merged = new IndexInfo();
        merged = merged.unionAll(Arrays.asList(left, right), null, false);
        
        assertEquals(140L, merged.count());
        assertTrue(merged.uids().isEmpty());
    }
    
    /**
     * Union of query terms when one term is infinite. The union of {inf, 3} is {inf}
     */
    @Test
    public void testUnion_InfiniteTerm() {
        IndexInfo left = new IndexInfo(-1L);
        left.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo right = new IndexInfo(Arrays.asList("doc1", "doc2", "doc3"));
        right.applyNode(JexlNodeFactory.buildEQNode("A", "1"));
        
        IndexInfo expectedMerged = new IndexInfo(-1L);
        
        // Left with right.
        IndexInfo merged = new IndexInfo();
        merged = merged.unionAll(Arrays.asList(left, right), null, true);
        assertEquals(expectedMerged, merged);
        
        // Right with left.
        merged = new IndexInfo();
        merged = merged.unionAll(Arrays.asList(right, left), null, true);
        assertEquals(expectedMerged, merged);
    }
}
