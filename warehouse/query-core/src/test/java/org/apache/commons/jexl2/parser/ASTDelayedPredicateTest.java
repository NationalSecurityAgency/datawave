package org.apache.commons.jexl2.parser;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    public void testDelayBoundedRange() throws ParseException {
        String query = "((_Bounded_ = true) && (SIZE >= 3 && SIZE <= 7))";
        String expected = "((_Delayed_ = true) && (((_Bounded_ = true) && (SIZE >= 3 && SIZE <= 7))))";
        
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlNode delayed = ASTDelayedPredicate.create(node);
        
        String delayedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(delayed);
        assertEquals(expected, delayedQuery);
    }
    
    @Test
    public void testDelayOfValueExceededBoundedRange() throws ParseException {
        String query = "((_Value_ = true) && (((_Bounded_ = true) && (SIZE >= 3 && SIZE <= 7))))";
        String expected = "((_Delayed_ = true) && (((_Value_ = true) && (((_Bounded_ = true) && (SIZE >= 3 && SIZE <= 7))))))";
        
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlNode delayed = ASTDelayedPredicate.create(node);
        
        String delayedQuery = JexlStringBuildingVisitor.buildQueryWithoutParse(delayed);
        assertEquals(expected, delayedQuery);
    }
}
