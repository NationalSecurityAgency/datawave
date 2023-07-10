package org.apache.commons.jexl2.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

public class ASTDelayedPredicateTest {

    @Test
    public void testDoNotDelayMultipleTimes() throws ParseException {
        String query = "FOO == 'bar'";
        String expected = "((_Delayed_ = true) && (FOO == 'bar'))";

        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        for (int i = 0; i < 15; i++) {
            node = ASTDelayedPredicate.create(node);
        }

        String delayedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        assertEquals(expected, delayedQuery);
    }

    // this verifies that getting the source node only unwraps the first layer
    @Test
    public void testGetSourceOfManyDelays() throws ParseException {
        String query = "((_Delayed_ = true) && ((_Delayed_ = true) && (FOO == 'bar')))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);

        String expectedSource = "(_Delayed_ = true) && (FOO == 'bar')";
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        JexlNode source = instance.getSource();
        assertEquals(expectedSource, JexlStringBuildingVisitor.buildQueryWithoutParse(source));

        expectedSource = "FOO == 'bar'";
        source = ASTDelayedPredicate.unwrapFully(node, ASTDelayedPredicate.class);
        assertEquals(expectedSource, JexlStringBuildingVisitor.buildQueryWithoutParse(source));
    }

    @Test
    public void testIsNodeAlreadyDelayed() throws ParseException {
        String query = "FOO == 'bar'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);

        assertNull(node.jjtGetParent());
        assertFalse(QueryPropertyMarker.isAncestorMarked(node, ASTDelayedPredicate.class));

        JexlNode delayed = ASTDelayedPredicate.create(node);
        assertNotNull(node.jjtGetParent());
        assertTrue(QueryPropertyMarker.findInstance(delayed).isType(ASTDelayedPredicate.class));
        assertTrue(ASTDelayedPredicate.isAncestorMarked(node, ASTDelayedPredicate.class));
    }

}
