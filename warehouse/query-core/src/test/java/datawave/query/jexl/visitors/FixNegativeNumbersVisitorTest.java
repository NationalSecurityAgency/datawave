package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FixNegativeNumbersVisitorTest {
    
    private static final Logger log = Logger.getLogger(FixNegativeNumbersVisitorTest.class);
    
    @Test
    public void testUnaryMinusModeConvertedToNumberLiteral() throws ParseException {
        String query = "FOO == -1";
        ASTJexlScript queryScript = JexlASTHelper.parseJexlQuery(query);
        
        // Verify the script was parsed with an unary minus node.
        assertTrue(queryScript.jjtGetChild(0).jjtGetChild(1) instanceof ASTUnaryMinusNode);
        
        ASTJexlScript fixed = FixNegativeNumbersVisitor.fix(queryScript);
        JexlNode convertedNode = fixed.jjtGetChild(0).jjtGetChild(1);
        
        // Verify the unary minus mode was converted to a number literal.
        assertTrue(convertedNode instanceof ASTNumberLiteral);
        assertEquals("-1", convertedNode.jjtGetValue());
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(fixed));
        
        // Verify the resulting script has a valid lineage.
        assertLineage(fixed);
        
        // Verify the original script was not modified, and has a valid lineage.
        assertScriptEquality(queryScript, query);
        assertLineage(queryScript);
    }
    
    private void assertScriptEquality(ASTJexlScript actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
