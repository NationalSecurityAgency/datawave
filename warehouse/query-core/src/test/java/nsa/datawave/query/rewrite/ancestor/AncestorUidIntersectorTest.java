package nsa.datawave.query.rewrite.ancestor;

import nsa.datawave.query.index.lookup.IndexMatch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.jexl2.parser.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class AncestorUidIntersectorTest {
    private AncestorUidIntersector intersector;
    private Set<IndexMatch> uids1;
    private Set<IndexMatch> uids2;
    private JexlNode node1;
    private JexlNode node2;
    
    @Before
    public void setup() {
        intersector = new AncestorUidIntersector();
        uids1 = new TreeSet<>();
        uids2 = new TreeSet<>();
        
        node1 = new ASTEQNode(1);
        JexlNode reference1 = new ASTReference(2);
        node1.jjtAddChild(reference1, 0);
        JexlNode name = new ASTIdentifier(3);
        name.image = "fieldName1";
        reference1.jjtAddChild(name, 0);
        
        JexlNode reference2 = new ASTReference(4);
        node1.jjtAddChild(reference2, 1);
        JexlNode value = new ASTStringLiteral(5);
        value.image = "fieldValue1";
        reference2.jjtAddChild(value, 0);
        
        node2 = new ASTEQNode(6);
        reference1 = new ASTReference(7);
        node2.jjtAddChild(reference1, 0);
        name = new ASTIdentifier(8);
        name.image = "fieldName2";
        reference1.jjtAddChild(name, 0);
        
        reference2 = new ASTReference(9);
        node2.jjtAddChild(reference2, 1);
        value = new ASTStringLiteral(10);
        value.image = "fieldValue2";
        reference2.jjtAddChild(value, 0);
    }
    
    @Test
    public void testCommonUids2() {
        uids1.add(new IndexMatch("a.b.c", node1));
        uids2.add(new IndexMatch("a.b.c.1.1", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c.1.1"));
    }
    
    @Test
    public void testCommonUids1() {
        uids1.add(new IndexMatch("a.b.c.1.1", node1));
        uids2.add(new IndexMatch("a.b.c", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c.1.1"));
    }
    
    @Test
    public void testSubstringNonAncestorUids1() {
        uids1.add(new IndexMatch("a.b.c.10.1", node1));
        uids2.add(new IndexMatch("a.b.c.1", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 0, got " + result.size(), result.size() == 0);
    }
    
    @Test
    public void testSubstringNonAncestorUids2() {
        uids1.add(new IndexMatch("a.b.c.1", node1));
        uids2.add(new IndexMatch("a.b.c.10.1", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 0, got " + result.size(), result.size() == 0);
    }
    
    @Test
    public void testReduceSingleUids1() {
        uids1.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1.1", node1));
        uids2.add(new IndexMatch("a.b.c.1.1.1", node2));
        uids2.add(new IndexMatch("a.b.c.1.1.2", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 2, got " + result.size(), result.size() == 2);
    }
    
    @Test
    public void testReduceOverlapUids1() {
        uids1.add(new IndexMatch("a.b.c.1", node1));
        
        uids2.add(new IndexMatch("a.b.c.1.1", node2));
        uids2.add(new IndexMatch("a.b.c.1.1.2", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c.1.1"));
    }
    
    @Test
    public void testReduceSingleUids2() {
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids2.add(new IndexMatch("a.b.c.1.1", node1));
        uids1.add(new IndexMatch("a.b.c.1.1.1", node2));
        uids1.add(new IndexMatch("a.b.c.1.1.2", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 2, got " + result.size(), result.size() == 2);
    }
    
    @Test
    public void testReduceOverlapUids2() {
        uids2.add(new IndexMatch("a.b.c.1", node1));
        
        uids1.add(new IndexMatch("a.b.c.1.1", node2));
        uids1.add(new IndexMatch("a.b.c.1.1.2", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c.1.1"));
    }
    
    @Test
    public void testEqualityUid() {
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c.1"));
        
    }
    
    @Test
    public void testEqualityUidOverlap() {
        uids2.add(new IndexMatch("a.b.c", node1));
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        uids1.add(new IndexMatch("a.b.c", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c"));
    }
    
    @Test
    public void testMultipleBranches() {
        uids2.add(new IndexMatch("a.b.c.2", node1));
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        uids1.add(new IndexMatch("a.b.c.2.1", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 2, got " + result.size(), result.size() == 2);
        List<IndexMatch> resultList = new ArrayList<>();
        for (IndexMatch match : result) {
            resultList.add(match);
        }
        List<IndexMatch> expected = new ArrayList<>();
        expected.add(new IndexMatch("a.b.c.1", null));
        expected.add(new IndexMatch("a.b.c.2.1", null));
        Assert.assertTrue(CollectionUtils.isEqualCollection(expected, resultList));
    }
    
    @Test
    public void testMultipleBranchesCommonAncestor() {
        uids2.add(new IndexMatch("a.b.c", node1));
        uids2.add(new IndexMatch("a.b.c.2", node1));
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        uids1.add(new IndexMatch("a.b.c.2.1", node2));
        uids1.add(new IndexMatch("a.b.c", node2));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 1, got " + result.size(), result.size() == 1);
        Assert.assertTrue(result.iterator().next().getUid().equals("a.b.c"));
    }
    
    @Test
    public void testEmptyUids1() {
        uids2.add(new IndexMatch("a.b.c", node1));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 0, got " + result.size(), result.size() == 0);
    }
    
    @Test
    public void testEmptyUids2() {
        uids1.add(new IndexMatch("a.b.c", node1));
        
        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);
        
        Assert.assertNotNull(result);
        Assert.assertTrue("expected size 0, got " + result.size(), result.size() == 0);
    }
}
