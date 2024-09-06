package datawave.query.ancestor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.junit.Before;
import org.junit.Test;

import datawave.query.index.lookup.IndexMatch;

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
        ASTIdentifier name = JexlNodes.makeIdentifier();
        JexlNodes.setIdentifier(name, "fieldName1");
        reference1.jjtAddChild(name, 0);

        JexlNode reference2 = new ASTReference(4);
        node1.jjtAddChild(reference2, 1);
        ASTStringLiteral value = JexlNodes.makeStringLiteral();
        JexlNodes.setLiteral(value, "fieldValue1");
        reference2.jjtAddChild(value, 0);

        node2 = new ASTEQNode(6);
        reference1 = new ASTReference(7);
        node2.jjtAddChild(reference1, 0);
        name = JexlNodes.makeIdentifier();
        JexlNodes.setIdentifier(name, "fieldName2");
        reference1.jjtAddChild(name, 0);

        reference2 = new ASTReference(9);
        node2.jjtAddChild(reference2, 1);
        value = JexlNodes.makeStringLiteral();
        JexlNodes.setLiteral(value, "fieldValue2");
        reference2.jjtAddChild(value, 0);
    }

    @Test
    public void testCommonUids2() {
        uids1.add(new IndexMatch("a.b.c", node1));
        uids2.add(new IndexMatch("a.b.c.1.1", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c.1.1", result.iterator().next().getUid());
    }

    @Test
    public void testCommonUids1() {
        uids1.add(new IndexMatch("a.b.c.1.1", node1));
        uids2.add(new IndexMatch("a.b.c", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c.1.1", result.iterator().next().getUid());
    }

    @Test
    public void testSubstringNonAncestorUids1() {
        uids1.add(new IndexMatch("a.b.c.10.1", node1));
        uids2.add(new IndexMatch("a.b.c.1", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertTrue("expected size 0, got " + result.size(), result.isEmpty());
        assertEquals("expected size 0, got " + result.size(), 0, result.size());
    }

    @Test
    public void testSubstringNonAncestorUids2() {
        uids1.add(new IndexMatch("a.b.c.1", node1));
        uids2.add(new IndexMatch("a.b.c.10.1", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertTrue("expected size 0, got " + result.size(), result.isEmpty());
        assertEquals("expected size 0, got " + result.size(), 0, result.size());
    }

    @Test
    public void testReduceSingleUids1() {
        uids1.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1.1", node1));
        uids2.add(new IndexMatch("a.b.c.1.1.1", node2));
        uids2.add(new IndexMatch("a.b.c.1.1.2", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 2, got " + result.size(), 2, result.size());
    }

    @Test
    public void testReduceOverlapUids1() {
        uids1.add(new IndexMatch("a.b.c.1", node1));

        uids2.add(new IndexMatch("a.b.c.1.1", node2));
        uids2.add(new IndexMatch("a.b.c.1.1.2", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c.1.1", result.iterator().next().getUid());
    }

    @Test
    public void testReduceSingleUids2() {
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids2.add(new IndexMatch("a.b.c.1.1", node1));
        uids1.add(new IndexMatch("a.b.c.1.1.1", node2));
        uids1.add(new IndexMatch("a.b.c.1.1.2", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 2, got " + result.size(), 2, result.size());
    }

    @Test
    public void testReduceOverlapUids2() {
        uids2.add(new IndexMatch("a.b.c.1", node1));

        uids1.add(new IndexMatch("a.b.c.1.1", node2));
        uids1.add(new IndexMatch("a.b.c.1.1.2", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c.1.1", result.iterator().next().getUid());
    }

    @Test
    public void testEqualityUid() {
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c.1", result.iterator().next().getUid());

    }

    @Test
    public void testEqualityUidOverlap() {
        uids2.add(new IndexMatch("a.b.c", node1));
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        uids1.add(new IndexMatch("a.b.c", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c", result.iterator().next().getUid());
    }

    @Test
    public void testMultipleBranches() {
        uids2.add(new IndexMatch("a.b.c.2", node1));
        uids2.add(new IndexMatch("a.b.c.1", node1));
        uids1.add(new IndexMatch("a.b.c.1", node2));
        uids1.add(new IndexMatch("a.b.c.2.1", node2));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertEquals("expected size 2, got " + result.size(), 2, result.size());
        List<IndexMatch> resultList = new ArrayList<>(result);
        List<IndexMatch> expected = new ArrayList<>();

        IndexMatch match1 = new IndexMatch("a.b.c.1");
        match1.add(node1);
        match1.add(node2);

        IndexMatch match2 = new IndexMatch("a.b.c.2.1");
        match2.add(node1);
        match2.add(node2);

        expected.add(match1);
        expected.add(match2);

        Iterator<IndexMatch> resultIter = resultList.iterator();
        Iterator<IndexMatch> expectedIter = expected.iterator();

        assertEquals(expectedIter.next(), resultIter.next());
        assertEquals(expectedIter.next(), resultIter.next());

        assertFalse(resultIter.hasNext());
        assertFalse(expectedIter.hasNext());
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

        assertNotNull(result);
        assertEquals("expected size 1, got " + result.size(), 1, result.size());
        assertEquals("a.b.c", result.iterator().next().getUid());
    }

    @Test
    public void testEmptyUids1() {
        uids2.add(new IndexMatch("a.b.c", node1));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertTrue("expected size 0, got " + result.size(), result.isEmpty());
        assertEquals("expected size 0, got " + result.size(), 0, result.size());
    }

    @Test
    public void testEmptyUids2() {
        uids1.add(new IndexMatch("a.b.c", node1));

        Set<IndexMatch> result = intersector.intersect(uids1, uids2, Collections.EMPTY_LIST);

        assertNotNull(result);
        assertTrue("expected size 0, got " + result.size(), result.isEmpty());
        assertEquals("expected size 0, got " + result.size(), 0, result.size());
    }
}
