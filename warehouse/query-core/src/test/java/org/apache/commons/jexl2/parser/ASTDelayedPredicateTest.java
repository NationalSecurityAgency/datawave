package org.apache.commons.jexl2.parser;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ASTDelayedPredicateTest {
    
    /**
     * The Jexl trees for an ASTDelayedPredicate are different.
     *
     * In the first case, the ASTDelayedPredicate is constructed one node at a time.
     *
     * In the second case, the ASTDelayedPredicate is constructed by parsing the Jexl tree from a string.
     *
     * @throws ParseException
     */
    @Test
    public void testNonEquivalentDelayedPredicates() throws ParseException {
        JexlNode node = JexlNodeFactory.buildEQNode("A", "1");
        JexlNode delayed = ASTDelayedPredicate.create(node);
        
        String expectedDelayed = "((ASTDelayedPredicate = true) && (A == '1'))";
        assertEquals(expectedDelayed, JexlStringBuildingVisitor.buildQuery(delayed));
        
        String delayedString = "((ASTDelayedPredicate = true) && (A == '1'))";
        ASTJexlScript parsedScript = JexlASTHelper.parseJexlQuery(delayedString);
        
        // Built strings are equal
        assertEquals(expectedDelayed, JexlStringBuildingVisitor.buildQuery(parsedScript));
        
        // Assert tree equality
        // ASTJexlScript delayedScript = JexlNodeFactory.createScript(delayed);
        
        // Non-equivalent ASTs. Uncomment and run to see the difference.
        // PrintingVisitor.printQuery(delayedScript);
        // System.out.println("=====================================");
        // PrintingVisitor.printQuery(parsedScript);
        // assertTrue(TreeEqualityVisitor.isEqual(delayedScript, parsedScript, new TreeEqualityVisitor.Reason()));
    }
}
