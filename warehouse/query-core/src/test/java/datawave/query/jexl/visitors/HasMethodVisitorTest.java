package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HasMethodVisitorTest {
    
    @Test
    public void testHasMethod() throws Exception {
        assertTrue(hasMethod("FOO.size() > 0"));
        assertTrue(hasMethod("AG.max() == 40"));
        assertTrue(hasMethod("BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'"));
        assertFalse(hasMethod("FOO == 'bar'"));
    }
    
    private boolean hasMethod(String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        return JexlASTHelper.HasMethodVisitor.hasMethod(script);
    }
}
