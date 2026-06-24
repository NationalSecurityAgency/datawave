package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

/**
 * Focused tests for {@link DocumentMatchFunctionVisitor}.
 */
public class DocumentMatchFunctionVisitorTest {

    /**
     * Verifies that the visitor reports when a query needs the reserved document-match context variable.
     *
     * @throws Exception
     *             if parsing fails
     */
    @Test
    public void testRewriteReportsWhetherDocumentMatchContextIsRequired() throws Exception {
        assertFalse(DocumentMatchFunctionVisitor.rewrite(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'")));
        assertTrue(DocumentMatchFunctionVisitor.rewrite(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar' && document:match('car')")));
    }

    @Test
    public void testRequiresDocumentMatchContext() throws Exception {
        assertFalse(DocumentMatchFunctionVisitor.requiresDocumentMatchContext(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'")));
        assertTrue(DocumentMatchFunctionVisitor.requiresDocumentMatchContext(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar' && document:match('car')")));
    }

    /**
     * Verifies that the one-argument form is rewritten to include the reserved context variable as the first argument.
     *
     * @throws ParseException
     *             if parsing fails
     */
    @Test
    public void testRewriteSingleArgumentFunction() throws ParseException {
        assertRewrite("document:match(documentMatchContext, 'car')", "document:match('car')");
    }

    /**
     * Verifies that the two-argument form keeps the view selector first and inserts the reserved context variable before the search string.
     *
     * @throws ParseException
     *             if parsing fails
     */
    @Test
    public void testRewriteTwoArgumentFunction() throws ParseException {
        assertRewrite("document:match('BODY', documentMatchContext, 'car')", "document:match('BODY', 'car')");
    }

    @Test
    public void testRewriteMutatesOriginalScript() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("document:match('car')");
        assertTrue(DocumentMatchFunctionVisitor.rewrite(script));
        assertEquals("document:match(documentMatchContext, 'car')", JexlStringBuildingVisitor.buildQueryWithoutParse(script));
    }

    /**
     * Verifies that the input form is rewritten the expected input.
     *
     * @param expected
     *            the expected re-written form
     * @param input
     *            the input to rewrite
     * @throws ParseException
     *             if parsing fails
     */
    private static void assertRewrite(String expected, String input) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(input);
        DocumentMatchFunctionVisitor.rewrite(script);
        String rewritten = JexlStringBuildingVisitor.buildQueryWithoutParse(script);
        assertEquals(expected, rewritten);
    }
}
