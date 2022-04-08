package org.apache.commons.jexl2.parser;

import datawave.query.jexl.JexlASTHelper;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JexlNodesTest {
    
    @Test
    public void testIsNodeNegated_queryHasNoNegation() throws ParseException {
        String query = "((FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        // parse the script into logical components
        JexlNode union = script.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);
        
        assertFalse(JexlNodes.isNodeNegated(union));
        assertFalse(JexlNodes.isNodeNegated(intersection));
        assertFalse(JexlNodes.isNodeNegated(fooNode));
        assertFalse(JexlNodes.isNodeNegated(textNode));
        assertFalse(JexlNodes.isNodeNegated(ageNode));
    }
    
    @Test
    public void testIsNodeNegated_topLevelNegation() throws ParseException {
        String query = "!((FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        JexlNode union = script.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);
        
        assertTrue(JexlNodes.isNodeNegated(union));
        assertTrue(JexlNodes.isNodeNegated(intersection));
        assertTrue(JexlNodes.isNodeNegated(fooNode));
        assertTrue(JexlNodes.isNodeNegated(textNode));
        assertTrue(JexlNodes.isNodeNegated(ageNode));
    }
    
    @Test
    public void testIsNodeNegated_subTreeNegated() throws ParseException {
        String query = "(!(FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        JexlNode union = script.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);
        
        assertFalse(JexlNodes.isNodeNegated(union));
        assertTrue(JexlNodes.isNodeNegated(intersection));
        assertTrue(JexlNodes.isNodeNegated(fooNode));
        assertTrue(JexlNodes.isNodeNegated(textNode));
        assertFalse(JexlNodes.isNodeNegated(ageNode));
    }
}
