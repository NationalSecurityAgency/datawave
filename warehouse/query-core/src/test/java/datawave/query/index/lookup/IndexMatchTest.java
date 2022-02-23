package datawave.query.index.lookup;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexMatchTest {
    
    @Test
    public void testSetOfIndexMatches() {
        JexlNode node = JexlNodeFactory.buildEQNode("FOO", "bar");
        JexlNode otherNode = JexlNodeFactory.buildEQNode("FOO2", "bar2");
        
        IndexMatch match1 = new IndexMatch("uid0");
        IndexMatch match2 = new IndexMatch("uid0");
        
        match1.add(node);
        match2.add(otherNode);
        
        Set<IndexMatch> matchSet = new HashSet<>();
        matchSet.add(match1);
        matchSet.add(match2);
        
        assertEquals(2, matchSet.size());
        Iterator<IndexMatch> matchIter = matchSet.iterator();
        assertEquals(Collections.singleton("FOO == 'bar'"), matchIter.next().nodeSet.getNodeKeys());
        assertEquals(Collections.singleton("FOO2 == 'bar2'"), matchIter.next().nodeSet.getNodeKeys());
    }
    
    @Test
    public void testEmptyConstructor() {
        IndexMatch indexMatch = new IndexMatch();
        // Assert via getters & direct access
        assertEquals("", indexMatch.getUid());
        assertNull(indexMatch.getNode());
        assertEquals("", indexMatch.shard);
        assertEquals("", indexMatch.uid);
        assertTrue(indexMatch.nodeSet.isEmpty());
        assertEquals(IndexMatchType.OR, indexMatch.type);
    }
    
    @Test
    public void testUidConstructor() {
        String uid = "uid";
        IndexMatch indexMatch = new IndexMatch(uid);
        // Assert via getters & direct access
        assertEquals(uid, indexMatch.getUid());
        assertEquals(uid, indexMatch.uid);
    }
    
    @Test
    public void testFineTuneConstructor() {
        String uid = "uid";
        JexlNode left = JexlNodeFactory.buildEQNode("FIELD", "left");
        JexlNode right = JexlNodeFactory.buildEQNode("FIELD", "right");
        Set<JexlNode> nodes = Sets.newHashSet(left, right);
        
        IndexMatch indexMatch = new IndexMatch(nodes, uid, IndexMatchType.AND);
        
        JexlNode andNode = JexlNodeFactory.createAndNode(nodes);
        assertEquals(uid, indexMatch.getUid());
        assertEquals(andNode.toString(), indexMatch.getNode().toString());
        assertEquals("", indexMatch.shard);
        assertEquals(uid, indexMatch.uid);
        assertEquals(Sets.newHashSet("FIELD == 'left'", "FIELD == 'right'"), indexMatch.nodeSet.getNodeKeys());
        assertEquals(nodes, Sets.newHashSet(indexMatch.nodeSet.getNodes()));
        assertEquals(IndexMatchType.AND, indexMatch.type);
    }
    
    @Test
    public void testTypeSet() {
        String uid = "uid";
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        IndexMatch indexMatch = new IndexMatch(uid, eqNode);
        
        assertEquals(IndexMatchType.OR, indexMatch.type);
        indexMatch.setType(IndexMatchType.AND);
        assertEquals(IndexMatchType.AND, indexMatch.type);
    }
    
    @Test
    public void testNodeSet() {
        String uid = "uid";
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        IndexMatch indexMatch = new IndexMatch(uid, eqNode);
        
        // Assert via getters & direct access
        assertEquals(eqNode, indexMatch.getNode());
        assertEquals(Sets.newHashSet("FIELD == 'value'"), indexMatch.nodeSet.getNodeKeys());
        assertEquals(Sets.newHashSet(eqNode), Sets.newHashSet(indexMatch.nodeSet.getNodes()));
        
        // Create & set other node
        JexlNode otherNode = JexlNodeFactory.buildEQNode("FIELD", "other");
        indexMatch.set(otherNode);
        
        // Assert via getters & direct access
        assertEquals(otherNode, indexMatch.getNode());
        assertEquals(Sets.newHashSet("FIELD == 'other'"), indexMatch.nodeSet.getNodeKeys());
        assertEquals(Sets.newHashSet(otherNode), Sets.newHashSet(indexMatch.nodeSet.getNodes()));
    }
    
    @Test
    public void testNodeAdditions() {
        String uid = "uid";
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        IndexMatch indexMatch = new IndexMatch(uid, eqNode);
        
        // Assert via getters & direct access
        assertEquals(eqNode, indexMatch.getNode());
        assertEquals(Sets.newHashSet("FIELD == 'value'"), indexMatch.nodeSet.getNodeKeys());
        assertEquals(Sets.newHashSet(eqNode), Sets.newHashSet(indexMatch.nodeSet.getNodes()));
        
        // Create nodes for addition
        JexlNode eqNode2 = JexlNodeFactory.buildEQNode("FIELD", "value2");
        JexlNode eqNode3 = JexlNodeFactory.buildEQNode("FIELD", "value3");
        
        // Add nodes
        indexMatch.add(eqNode2);
        indexMatch.add(eqNode3);
        
        // Create expected objects
        Set<String> expectedStrings = Sets.newHashSet("FIELD == 'value'", "FIELD == 'value2'", "FIELD == 'value3'");
        Collection<JexlNode> expectedNodes = Sets.newHashSet(eqNode, eqNode2, eqNode3);
        JexlNode orNode = JexlNodeFactory.createUnwrappedOrNode(expectedNodes);
        
        assertEquals(expectedStrings, indexMatch.nodeSet.getNodeKeys());
        assertEquals(expectedNodes, Sets.newHashSet(indexMatch.nodeSet.getNodes()));
        assertEquals(orNode.toString(), indexMatch.getNode().toString());
    }
    
    @Test
    public void testToString() {
        String uid = "uid";
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        IndexMatch indexMatch = new IndexMatch(uid, eqNode);
        
        String expectedString = "uid - {FIELD == 'value' }";
        assertEquals(expectedString, indexMatch.toString());
    }
    
    @Test
    public void testEquals() {
        IndexMatch left = new IndexMatch("uid0");
        IndexMatch right = new IndexMatch("uid0");
        
        assertEquals(left, right);
        assertEquals(right, left);
        
        assertNotEquals(left, null);
    }
    
    @Test
    public void testEqualsWithDifferentUids() {
        IndexMatch left = new IndexMatch("uid0");
        IndexMatch right = new IndexMatch("uid2");
        
        assertNotEquals(left, right);
        assertNotEquals(right, left);
    }
    
    @Test
    public void testEqualsWithDifferentShards() {
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        
        IndexMatch left = new IndexMatch("uid0", eqNode, "20190413_0");
        IndexMatch right = new IndexMatch("uid0", eqNode, "20190413_1");
        
        assertEquals(left, right);
        assertEquals(right, left);
    }
    
    @Test
    public void testEqualsWithDifferentJexlNodes() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD0", "value0");
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "value2");
        
        IndexMatch left = new IndexMatch("uid0", leftNode);
        IndexMatch right = new IndexMatch("uid0", rightNode);
        
        assertNotEquals(left, right);
        assertNotEquals(right, left);
    }
    
    @Test
    public void testCompareTo() {
        IndexMatch left = new IndexMatch("uid0");
        IndexMatch right = new IndexMatch("uid0");
        
        assertEquals(0, left.compareTo(right));
        assertEquals(0, right.compareTo(left));
    }
    
    @Test
    public void testCompareToWithDifferentUids() {
        IndexMatch left = new IndexMatch("uid0");
        IndexMatch right = new IndexMatch("uid2");
        
        assertEquals(-2, left.compareTo(right));
        assertEquals(2, right.compareTo(left));
    }
    
    @Test
    public void testCompareToWithDifferentShards() {
        JexlNode eqNode = JexlNodeFactory.buildEQNode("FIELD", "value");
        
        IndexMatch left = new IndexMatch("uid0", eqNode, "20190413_0");
        IndexMatch right = new IndexMatch("uid0", eqNode, "20190413_1");
        
        assertEquals(0, left.compareTo(right));
        assertEquals(0, right.compareTo(left));
    }
    
    @Test
    public void testCompareToWithDifferentJexlNodes() {
        JexlNode leftNode = JexlNodeFactory.buildEQNode("FIELD0", "value0");
        JexlNode rightNode = JexlNodeFactory.buildEQNode("FIELD2", "value2");
        
        IndexMatch left = new IndexMatch("uid0", leftNode);
        IndexMatch right = new IndexMatch("uid0", rightNode);
        
        assertEquals(0, left.compareTo(right));
        assertEquals(0, right.compareTo(left));
    }
    
    @Test
    public void testHashCode() {
        String uid = "uid";
        IndexMatch indexMatch = new IndexMatch(uid);
        assertEquals(uid.hashCode(), indexMatch.hashCode());
    }
    
    @Test
    public void testWriteReadIndexMatch() throws IOException {
        IndexMatch match = new IndexMatch("uid");
        
        IndexMatch other = writeRead(match);
        assertEquals("uid", other.uid);
    }
    
    @Test(expected = NullPointerException.class)
    public void testWriteReadIndexMatchWithNullUid() throws IOException {
        IndexMatch match = new IndexMatch(null);
        
        // Cannot write a null uid.
        IndexMatch other = writeRead(match);
        fail("IndexMatch should have thrown an exception trying to write a null uid.");
    }
    
    /**
     * Write the provided IndexMatch to a byte array and construct a fresh IndexMatch from that byte array.
     *
     * @param match
     *            the IndexMatch to be written to a byte array.
     * @return a new IndexMatch constructed from the byte array
     * @throws IOException
     */
    private IndexMatch writeRead(IndexMatch match) throws IOException {
        // Write the IndexMatch to a byte array.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        match.write(output);
        outputStream.flush();
        
        // Construct input stream from byte array.
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        
        // Construct IndexMatch from input stream and return.
        IndexMatch other = new IndexMatch();
        other.readFields(input);
        return other;
    }
}
