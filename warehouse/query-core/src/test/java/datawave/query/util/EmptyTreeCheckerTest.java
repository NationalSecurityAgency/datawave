package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.junit.jupiter.api.Test;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;

public class EmptyTreeCheckerTest {

    /**
     * Root node is non-null and has children
     */
    @Test
    public void testValidTree() {
        JexlNode node = parse("FOO == 'bar'");
        assertValid(node);

        node = parse("(FOO == 'bar' || FOO == 'baz')");
        assertValid(node);
    }

    /**
     * Root nodes either have no children or are null
     */
    @Test
    public void testInvalidTree() {
        ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        assertInvalid(script);

        ASTReferenceExpression referenceExpression = JexlNodes.makeRefExp();
        assertInvalid(referenceExpression);

        JexlNode eq = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        assertInvalid(eq);

        assertInvalid(null);
    }

    private void assertValid(JexlNode node) {
        assertDoesNotThrow(() -> EmptyTreeChecker.check(node));
    }

    private void assertInvalid(JexlNode node) {
        assertThrows(DatawaveFatalQueryException.class, () -> EmptyTreeChecker.check(node));
    }

    private JexlNode parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("bad query: " + query);
            throw new RuntimeException("bad query");
        }
    }
}
