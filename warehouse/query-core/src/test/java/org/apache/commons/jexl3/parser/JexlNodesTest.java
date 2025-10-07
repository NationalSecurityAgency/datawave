package org.apache.commons.jexl3.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class JexlNodesTest {

    @Test
    public void testIsNodeNegated_queryHasNoNegation() throws ParseException {
        String query = "((FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        // parse the script into logical components
        JexlNode union = script.jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);

        assertFalse(JexlNodes.findNegatedParent(union));
        assertFalse(JexlNodes.findNegatedParent(intersection));
        assertFalse(JexlNodes.findNegatedParent(fooNode));
        assertFalse(JexlNodes.findNegatedParent(textNode));
        assertFalse(JexlNodes.findNegatedParent(ageNode));
    }

    @Test
    public void testIsNodeNegated_topLevelNegation() throws ParseException {
        String query = "!((FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        JexlNode union = script.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);

        assertTrue(JexlNodes.findNegatedParent(union));
        assertTrue(JexlNodes.findNegatedParent(intersection));
        assertTrue(JexlNodes.findNegatedParent(fooNode));
        assertTrue(JexlNodes.findNegatedParent(textNode));
        assertTrue(JexlNodes.findNegatedParent(ageNode));
    }

    @Test
    public void testIsNodeNegated_subTreeNegated() throws ParseException {
        String query = "(!(FOO == 'bar' && TEXT == 'text') || AGE == '25')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        JexlNode union = script.jjtGetChild(0).jjtGetChild(0);
        JexlNode intersection = union.jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        JexlNode fooNode = intersection.jjtGetChild(0);
        JexlNode textNode = intersection.jjtGetChild(1);
        JexlNode ageNode = union.jjtGetChild(1);

        assertFalse(JexlNodes.findNegatedParent(union));
        assertTrue(JexlNodes.findNegatedParent(intersection));
        assertTrue(JexlNodes.findNegatedParent(fooNode));
        assertTrue(JexlNodes.findNegatedParent(textNode));
        assertFalse(JexlNodes.findNegatedParent(ageNode));
    }
}
