package datawave.query.language.parser.jexl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JexlNodeSetTest {
    
    @Test
    public void testTheReasonForThisClass() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO", "bar");
        
        Set<JexlNode> nodeSet = Sets.newHashSet(node1, node2);
        assertEquals(2, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
    }
    
    @Test
    public void testGetNodesAndGetNodeKeys() {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        String key = "FOO == 'bar'";
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node);
        
        assertEquals(Collections.singleton(key), nodeSet.getNodeKeys());
        assertEquals(node, nodeSet.getNodes().iterator().next());
    }
    
    @Test
    public void testContains() {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node);
        assertTrue(nodeSet.contains(node));
        
        // Nope, it won't.
        assertFalse(nodeSet.contains("a string?!?!"));
    }
    
    @Test
    public void testIterator() {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node);
        
        Iterator<JexlNode> iter = nodeSet.iterator();
        assertTrue(iter.hasNext());
        JexlNode next = iter.next();
        assertEquals(JexlASTHelper.nodeToKey(node), JexlASTHelper.nodeToKey(next));
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testToArray1() {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node);
        
        Object[] array = nodeSet.toArray();
        assertEquals(1, array.length);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testToArray2() {
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.toArray(new Object[0]);
    }
    
    @Test
    public void testAdd() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        assertTrue(nodeSet.add(node1));
        assertFalse(nodeSet.add(node2));
        
        assertEquals(1, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
        
        assertTrue(nodeSet.add(node3));
        assertFalse(nodeSet.add(node3));
        assertFalse(nodeSet.add(node3));
        
        assertEquals(2, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
    }
    
    @Test
    public void testRemove() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node1);
        nodeSet.add(node2);
        
        assertEquals(2, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        
        // Remove node 2
        assertTrue(nodeSet.remove(node2));
        assertEquals(1, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        
        // Remove node 1
        assertTrue(nodeSet.remove(node1));
        assertEquals(0, nodeSet.size());
        assertFalse(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        assertTrue(nodeSet.isEmpty());
        
        // Try to remove something else
        assertFalse(nodeSet.remove("this other thing"));
    }
    
    @Test
    public void testContainsAll() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO3", "bar3");
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.add(node1);
        nodeSet.add(node2);
        
        // Test true condition
        Collection<JexlNode> nodes = Lists.newArrayList(node1, node2);
        assertTrue(nodeSet.containsAll(nodes));
        
        // Test false condition 1, collection has some overlap.
        nodes = Lists.newArrayList(node2, node3);
        assertFalse(nodeSet.containsAll(nodes));
        
        // Test false condition 2, collection with no overlap.
        nodes = Lists.newArrayList(node3);
        assertFalse(nodeSet.containsAll(nodes));
        
        // Test null case
        assertFalse(nodeSet.containsAll(null));
    }
    
    @Test
    public void testAddAll() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO3", "bar3");
        
        Collection<JexlNode> first = Lists.newArrayList(node1, node2);
        Collection<JexlNode> second = Lists.newArrayList(node2, node3);
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        assertTrue(nodeSet.addAll(first));
        
        assertEquals(2, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
        
        assertFalse(nodeSet.addAll(first));
        assertEquals(2, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
        
        assertTrue(nodeSet.addAll(second));
        assertEquals(3, nodeSet.size());
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
    }
    
    @Test
    public void testRetainAll() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO3", "bar3");
        
        Collection<JexlNode> nodes = Lists.newArrayList(node1, node2, node3);
        Collection<JexlNode> retain = Lists.newArrayList(node1, node3);
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.addAll(nodes);
        
        // Retain nodes 1 and 3
        assertTrue(nodeSet.retainAll(retain));
        assertTrue(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
        
        // Try to retain same set, no change expected
        assertFalse(nodeSet.retainAll(retain));
        assertTrue(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
        
        // Retain node 1.
        assertTrue(nodeSet.retainAll(Collections.singletonList(node1)));
        assertTrue(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
    }
    
    @Test
    public void testRemoveAll() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO3", "bar3");
        
        Collection<JexlNode> nodes = Lists.newArrayList(node1, node2, node3);
        Collection<JexlNode> remove = Lists.newArrayList(node1, node3);
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.addAll(nodes);
        
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
        
        assertTrue(nodeSet.removeAll(remove));
        assertFalse(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
    }
    
    @Test
    public void testClear() {
        JexlNode node1 = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode node2 = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        JexlNode node3 = JexlNodeFactory.buildEQNode("FOO3", "bar3");
        
        Collection<JexlNode> nodes = Lists.newArrayList(node1, node2, node3);
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        nodeSet.addAll(nodes);
        
        assertTrue(nodeSet.contains(node1));
        assertTrue(nodeSet.contains(node2));
        assertTrue(nodeSet.contains(node3));
        assertFalse(nodeSet.isEmpty());
        
        nodeSet.clear();
        assertFalse(nodeSet.contains(node1));
        assertFalse(nodeSet.contains(node2));
        assertFalse(nodeSet.contains(node3));
        assertTrue(nodeSet.isEmpty());
    }
}
