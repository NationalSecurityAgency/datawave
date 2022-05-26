package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IvaratorRequiredVisitorTest {

    @Test
    public void testExceededOrThresholdTrue() throws ParseException {
        String query = "((_List_ = true) && (FOO == 1 && FOO == 3))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertTrue(IvaratorRequiredVisitor.isIvaratorRequired(script));
    }

    @Test
    public void testExceededThresholdFalse() throws ParseException {
        String query = "(FOO == 1 && FOO == 3)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertFalse(IvaratorRequiredVisitor.isIvaratorRequired(script));
    }

    @Test
    public void testExceededValueThresholdTrue() throws ParseException {
        String query = "((_Value_ = true) && (FOO == 1 || FOO == 3))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertTrue(IvaratorRequiredVisitor.isIvaratorRequired(script));
    }
    
}
