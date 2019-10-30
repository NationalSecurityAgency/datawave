package datawave.query.index.lookup;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;

import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static datawave.query.index.lookup.IndexInfoUnion.union;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test all combinations of document and infinite ranges, both with and without delayed nodes.
 */
public class IndexInfoUnionTest {
    
    @Test
    public void testUnionOfDocumentRanges_sameTerm_sameUids() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 3L, 3, Sets.newHashSet(leftNode));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 1, "a.b.c", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "b.c.d", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "c.d.e", "FIELD1 == 'VALUE1'");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_sameTerm_sameUids_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 3L, 3, Sets.newHashSet(leftNode, delayed));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 2, "a.b.c", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 2, "b.c.d", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 2, "c.d.e", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_sameTerm_differentUids() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(Sets.newHashSet("b.c.d", "c.d.e", "d.e.f"));
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 4L, 4, Sets.newHashSet(leftNode));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 1, "a.b.c", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "b.c.d", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "c.d.e", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "d.e.f", "FIELD1 == 'VALUE1'");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_sameTerm_differentUids_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(Sets.newHashSet("b.c.d", "c.d.e", "d.e.f"));
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 4L, 4, Sets.newHashSet(leftNode, delayed));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 2, "a.b.c", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 2, "b.c.d", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 2, "c.d.e", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 2, "d.e.f", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_differentTerm_sameUids() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 3L, 3, Sets.newHashSet(leftNode, rightNode));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 2, "a.b.c", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "b.c.d", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "c.d.e", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_differentTerm_sameUids_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 3L, 3, Sets.newHashSet(leftNode, rightNode, delayed));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 3, "a.b.c", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 3, "b.c.d", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 3, "c.d.e", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRanges_differentTerm_differentUids() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(Sets.newHashSet("b.c.d", "c.d.e", "d.e.f"));
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 4L, 4, Sets.newHashSet(leftNode, rightNode));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 1, "a.b.c", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 2, "b.c.d", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "c.d.e", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 1, "d.e.f", "FIELD2 == 'VALUE2'");
        
        assertFalse(uidIter.hasNext());
        
        // Take the union of the existing union and a new IndexInfo
        JexlNode centerNode = JexlNodeFactory.buildEQNode("FIELD3", "VALUE3");
        IndexInfo center = new IndexInfo(Sets.newHashSet("c.d.e", "d.e.f", "e.f.g"));
        center.applyNode(centerNode);
        
        union = union(union, center);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 5L, 5, Sets.newHashSet(leftNode, rightNode, centerNode));
        
        // Assert correct union of IndexMatches
        uidIter = union.uids.iterator();
        
        // Assert correctness of each IndexMatch
        assertIndexMatch(uidIter, 1, "a.b.c", "FIELD1 == 'VALUE1'");
        assertIndexMatch(uidIter, 1, "b.c.d", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "c.d.e", "(FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2' || FIELD3 == 'VALUE3')");
        assertIndexMatch(uidIter, 2, "d.e.f", "(FIELD2 == 'VALUE2' || FIELD3 == 'VALUE3')");
        assertIndexMatch(uidIter, 1, "e.f.g", "FIELD3 == 'VALUE3'");
        
        assertFalse(uidIter.hasNext());
    }
    
    // TODO -- this exposes a special case where duplicate delayed nodes will get added to the IndexMatch object.
    @Test
    public void testUnionOfDocumentRanges_differentTerm_differentUids_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(Sets.newHashSet("b.c.d", "c.d.e", "d.e.f"));
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 4L, 4, Sets.newHashSet(leftNode, rightNode, delayed));
        
        // Assert correct union of IndexMatches
        Iterator<IndexMatch> uidIter = union.uids.iterator();
        assertIndexMatch(uidIter, 2, "a.b.c", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 3, "b.c.d", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 3, "c.d.e", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "d.e.f", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD2 == 'VALUE2')");
        
        assertFalse(uidIter.hasNext());
        
        // Take the union of the existing union and a new IndexInfo
        JexlNode centerNode = JexlNodeFactory.buildEQNode("FIELD3", "VALUE3");
        IndexInfo center = new IndexInfo(Sets.newHashSet("c.d.e", "d.e.f", "e.f.g"));
        center.applyNode(centerNode);
        
        union = union(union, center, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, 5L, 5, Sets.newHashSet(leftNode, rightNode, delayed, centerNode));
        
        // Assert correct union of IndexMatches
        uidIter = union.uids.iterator();
        
        // Assert correctness of each IndexMatch
        assertIndexMatch(uidIter, 1, "a.b.c", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1')");
        assertIndexMatch(uidIter, 1, "b.c.d", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "c.d.e",
                        "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD1 == 'VALUE1' || FIELD2 == 'VALUE2' || FIELD3 == 'VALUE3')");
        assertIndexMatch(uidIter, 2, "d.e.f", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD3 == 'VALUE3' || FIELD2 == 'VALUE2')");
        assertIndexMatch(uidIter, 2, "e.f.g", "(((ASTDelayedPredicate = true) && (FIELD_X == 'VALUE_X')) || FIELD3 == 'VALUE3')");
        
        assertFalse(uidIter.hasNext());
    }
    
    @Test
    public void testUnionOfDocumentRangeAndInfiniteRange_sameTerm() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode));
    }
    
    @Test
    public void testUnionOfDocumentRangeAndInfiniteRange_sameTerm_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, delayed));
    }
    
    @Test
    public void testUnionOfDocumentRangeAndInfiniteRange_differentTerm() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode));
    }
    
    @Test
    public void testUnionOfDocumentRangeAndInfiniteRange_differentTerm_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(Sets.newHashSet("a.b.c", "b.c.d", "c.d.e"));
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode, delayed));
    }
    
    @Test
    public void testUnionOfTwoInfiniteRanges_sameTerm() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(-1);
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode));
    }
    
    @Test
    public void testUnionOfTwoInfiniteRanges_sameTerm_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(-1);
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, delayed));
    }
    
    @Test
    public void testUnionOfTwoInfiniteRanges_differentTerm() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(-1);
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        IndexInfo union = union(left, right);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode));
        
        // Take the union of the existing union and a new IndexInfo
        JexlNode centerNode = JexlNodeFactory.buildEQNode("FIELD3", "VALUE3");
        IndexInfo center = new IndexInfo(-1);
        center.applyNode(centerNode);
        
        union = union(union, center);
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode, centerNode));
    }
    
    @Test
    public void testUnionOfTwoInfiniteRanges_differentTerm_withDelayedNode() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD1", "VALUE1");
        IndexInfo left = new IndexInfo(-1);
        left.applyNode(leftNode);
        
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "VALUE2");
        IndexInfo right = new IndexInfo(-1);
        right.applyNode(rightNode);
        
        JexlNode gonnaBeLate = JexlNodeFactory.buildEQNode("FIELD_X", "VALUE_X");
        JexlNode delayed = ASTDelayedPredicate.create(gonnaBeLate);
        
        IndexInfo union = union(left, right, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode, delayed));
        
        // Take the union of the existing union and a new IndexInfo
        JexlNode centerNode = JexlNodeFactory.buildEQNode("FIELD3", "VALUE3");
        IndexInfo center = new IndexInfo(-1);
        center.applyNode(centerNode);
        
        union = union(union, center, Collections.singletonList(delayed));
        
        // Assert correct IndexInfo post-union
        assertIndexInfo(union, -1L, 0, Sets.newHashSet(leftNode, rightNode, delayed, centerNode));
    }
    
    private void assertIndexMatch(Iterator<IndexMatch> iter, int nodeCount, String uid, String expectedQuery) {
        IndexMatch match = iter.next();
        
        // Assert that the IndexMatch uid is correct.
        assertEquals(uid, match.uid);
        
        // Assert that the IndexMatch jexl node list is correctly sized.
        assertEquals(nodeCount, match.myNodes.size());
        
        // Assert that the IndexMatch jexl node list is correct.
        assertJexlNodeListEquality(match.getNode(), expectedQuery);
    }
    
    private void assertJexlNodeListEquality(JexlNode originalNode, String expected) {
        try {
            String original = JexlStringBuildingVisitor.buildQueryWithoutParse(originalNode);
            
            // Query strings may be unordered.
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
            
            // System.out.println("A: " + JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript));
            // System.out.println("B: " + JexlStringBuildingVisitor.buildQueryWithoutParse(originalScript));
            
            assertTrue(TreeEqualityVisitor.isEqual(expectedScript, originalScript, new TreeEqualityVisitor.Reason()));
        } catch (ParseException e) {
            e.printStackTrace();
            fail("Failed to parse a query string");
        }
    }
    
    private void assertIndexInfo(IndexInfo union, long count, int uidSize, Set<JexlNode> expectedNodes) {
        // Assert uid count
        assertEquals(count, union.count);
        
        // Assert node count
        assertEquals(uidSize, union.uids.size());
        
        // IndexInfo.getNode() creates an unwrapped AND/OR node, replicate that here.
        JexlNode expectedNode = JexlNodeFactory.createUnwrappedOrNode(expectedNodes);
        ASTJexlScript expectedScript = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createScript(expectedNode));
        ASTJexlScript originalScript = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createScript(union.getNode()));
        
        // System.out.println("A: " + JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript));
        // System.out.println("B: " + JexlStringBuildingVisitor.buildQueryWithoutParse(originalScript));
        
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, originalScript, new TreeEqualityVisitor.Reason()));
    }
}
