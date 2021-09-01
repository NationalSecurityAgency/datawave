package org.apache.commons.jexl2.parser;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ASTDelayedPredicateTest {
    
    @Test
    public void testMultipleDelays() throws ParseException {
        String query = "FOO == 'bar'";
        String expected = "((_Delayed_ = true) && (FOO == 'bar'))";
        
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        for (int i = 0; i < 15; i++) {
            node = ASTDelayedPredicate.create(node);
        }
        
        String delayedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        assertEquals(expected, delayedQuery);
    }
    
    @Test
    public void testIsNodeAlreadyDelayed() throws ParseException {
        String query = "FOO == 'bar'";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        
        assertNull(node.jjtGetParent());
        assertFalse(ASTDelayedPredicate.isSubTreeAlreadyDelayed(node));
        
        JexlNode delayed = ASTDelayedPredicate.create(node);
        assertNotNull(node.jjtGetParent());
        assertTrue(ASTDelayedPredicate.instanceOf(delayed));
        assertTrue(ASTDelayedPredicate.isSubTreeAlreadyDelayed(node));
    }
}
