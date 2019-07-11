package datawave.query.jexl.nodes;

import com.google.common.collect.Lists;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Test;

import static datawave.query.jexl.JexlNodeFactory.buildEQNode;
import static datawave.query.jexl.visitors.JexlStringBuildingVisitor.buildQueryWithoutParse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TreeHashNodeTest {
    
    @Test
    public void testDifferentObjectsSameValuesSameHash() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "bar");
        
        TreeHashNode leftHashNode = new TreeHashNode();
        leftHashNode.append(buildQueryWithoutParse(eq01));
        
        TreeHashNode rightHashNode = new TreeHashNode();
        rightHashNode.append(buildQueryWithoutParse(eq02));
        
        assertEquals(leftHashNode.hashCode(), rightHashNode.hashCode());
    }
    
    @Test
    public void testOneHasMoreNodes() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "baz");
        JexlNode eq03 = buildEQNode("FOO", "byzantium");
        
        TreeHashNode leftHashNode = new TreeHashNode();
        leftHashNode.append(buildQueryWithoutParse(eq01));
        leftHashNode.append(buildQueryWithoutParse(eq02));
        
        TreeHashNode rightHashNode = new TreeHashNode();
        rightHashNode.append(buildQueryWithoutParse(eq01));
        rightHashNode.append(buildQueryWithoutParse(eq02));
        rightHashNode.append(buildQueryWithoutParse(eq03));
        
        assertNotEquals(leftHashNode.hashCode(), rightHashNode.hashCode());
    }
    
    @Test
    public void testRightHasDuplicateNodes() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "baz");
        
        TreeHashNode leftHashNode = new TreeHashNode();
        leftHashNode.append(buildQueryWithoutParse(eq01));
        leftHashNode.append(buildQueryWithoutParse(eq02));
        
        TreeHashNode rightHashNode = new TreeHashNode();
        rightHashNode.append(buildQueryWithoutParse(eq01));
        rightHashNode.append(buildQueryWithoutParse(eq02));
        rightHashNode.append(buildQueryWithoutParse(eq02));
        
        assertNotEquals(leftHashNode.hashCode(), rightHashNode.hashCode());
    }
    
    @Test
    public void testOrderOfAddition() {
        JexlNode eq01 = buildEQNode("FOO", "bar");
        JexlNode eq02 = buildEQNode("FOO", "baz");
        
        JexlNode or01 = JexlNodeFactory.createOrNode(Lists.newArrayList(eq01, eq02));
        JexlNode or02 = JexlNodeFactory.createOrNode(Lists.newArrayList(eq02, eq01));
        
        TreeHashNode leftHashNode = new TreeHashNode();
        TreeHashNode rightHashNode = new TreeHashNode();
        
        leftHashNode.append(buildQueryWithoutParse(or01));
        rightHashNode.append(buildQueryWithoutParse(or01));
        
        // Same ORNode should match
        assertEquals(leftHashNode.hashCode(), rightHashNode.hashCode());
        
        leftHashNode = new TreeHashNode();
        rightHashNode = new TreeHashNode();
        
        leftHashNode.append(buildQueryWithoutParse(or01));
        rightHashNode.append(buildQueryWithoutParse(or02));
        
        // OR nodes with the same, but unordered children will not mach.
        assertNotEquals(leftHashNode.hashCode(), rightHashNode.hashCode());
    }
}
