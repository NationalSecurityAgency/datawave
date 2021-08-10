package datawave.query.jexl;

import com.google.common.collect.Sets;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.Node;
import org.junit.Test;

import java.util.Set;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class NodeTypeCountTest {
    
    @Test
    public void testPresentTypeBehavior() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        count.increment(ASTJexlScript.class);
        
        assertEquals(2, count.getTotal(ASTJexlScript.class));
        assertTrue(count.isPresent(ASTJexlScript.class));
    }
    
    @Test
    public void testMissingTypeBehavior() {
        NodeTypeCount count = new NodeTypeCount();
        assertEquals(0, count.getTotal(ASTJexlScript.class));
        assertFalse(count.isPresent(ASTJexlScript.class));
    }
    
    @Test
    public void testGetTotalNodes() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        count.increment(ASTJexlScript.class);
        count.increment(ASTJexlScript.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTAndNode.class);
        
        assertEquals(6, count.getTotalNodes());
    }
    
    @Test
    public void testGetTotalDistinctTypes() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        count.increment(ASTJexlScript.class);
        count.increment(ASTJexlScript.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTAndNode.class);
        
        assertEquals(2, count.getTotalDistinctTypes());
    }
    
    @Test
    public void testHasAny() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        
        Set<Class<? extends Node>> containsPresentType = Sets.newHashSet(ASTJexlScript.class, ASTAndNode.class, ASTOrNode.class);
        assertTrue(count.hasAny(ASTJexlScript.class, ASTAndNode.class, ASTOrNode.class));
        assertTrue(count.hasAny(containsPresentType));
        assertTrue(count.hasAny(containsPresentType.stream()));
        
        Set<Class<? extends Node>> onlyMissingTypes = Sets.newHashSet(ASTFalseNode.class, ASTAndNode.class, ASTOrNode.class);
        assertFalse(count.hasAny(ASTFalseNode.class, ASTAndNode.class, ASTOrNode.class));
        assertFalse(count.hasAny(onlyMissingTypes));
        assertFalse(count.hasAny(onlyMissingTypes.stream()));
    }
    
    @Test
    public void testHasAll() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTOrNode.class);
        
        Set<Class<? extends Node>> onlyPresentTypes = Sets.newHashSet(ASTJexlScript.class, ASTAndNode.class, ASTOrNode.class);
        assertTrue(count.hasAll(ASTJexlScript.class, ASTAndNode.class, ASTOrNode.class));
        assertTrue(count.hasAll(onlyPresentTypes));
        assertTrue(count.hasAll(onlyPresentTypes.stream()));
        
        Set<Class<? extends Node>> containsMissingType = Sets.newHashSet(ASTFalseNode.class, ASTAndNode.class, ASTOrNode.class);
        assertFalse(count.hasAll(ASTFalseNode.class, ASTAndNode.class, ASTOrNode.class));
        assertFalse(count.hasAll(containsMissingType));
        assertFalse(count.hasAll(containsMissingType.stream()));
    }
    
    @Test
    public void testToPrettyString() {
        NodeTypeCount count = new NodeTypeCount();
        count.increment(ASTJexlScript.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTAndNode.class);
        count.increment(ASTOrNode.class);
        count.increment(ASTOrNode.class);
        count.increment(ASTOrNode.class);
        
        String expected = "org.apache.commons.jexl2.parser.ASTAndNode: 2\n" + "org.apache.commons.jexl2.parser.ASTJexlScript: 1\n"
                        + "org.apache.commons.jexl2.parser.ASTOrNode: 3";
        assertEquals(expected, count.toPrettyString());
    }
}
