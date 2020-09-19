package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FixNegativeNumbersVisitorTest {
    
    @Test
    public void testUnaryMinusModeConvertedToNumberLiteral() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == -1");
        ASTJexlScript fixed = FixNegativeNumbersVisitor.fix(script);
        JexlNode convertedNode = fixed.jjtGetChild(0).jjtGetChild(1);
        
        assertTrue(convertedNode instanceof ASTNumberLiteral);
        assertEquals("-1", convertedNode.jjtGetValue());
        assertEquals("FOO == -1", JexlStringBuildingVisitor.buildQuery(fixed));
        assertTrue(JexlASTHelper.validateLineage(fixed, true));
    }
}
