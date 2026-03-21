package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

/**
 * Focused tests for {@link DocumentMatchFunctionRebuildingVisitor}.
 */
public class DocumentMatchFunctionRebuildingVisitorTest {

    /**
     * Verifies that the visitor reports when a query needs the reserved document-match context variable.
     *
     * @throws Exception
     *             if parsing fails
     */
    @Test
    public void testRequiresDocumentMatchContext() throws Exception {
        assertFalse(DocumentMatchFunctionRebuildingVisitor.requiresDocumentMatchContext(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'")));
        assertTrue(DocumentMatchFunctionRebuildingVisitor
                        .requiresDocumentMatchContext(JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar' && document:match('car')")));
    }

    /**
     * Verifies that the one-argument form is rewritten to include the reserved context variable as the first argument.
     *
     * @throws Exception
     *             if parsing fails
     */
    @Test
    public void testRewriteSingleArgumentFunction() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("document:match('car')");
        String rewritten = JexlStringBuildingVisitor.buildQueryWithoutParse(DocumentMatchFunctionRebuildingVisitor.rewrite(script));
        assertEquals("document:match(documentMatchContext, 'car')", rewritten);
    }

    /**
     * Verifies that the two-argument form keeps the view selector first and inserts the reserved context variable before the search string.
     *
     * @throws Exception
     *             if parsing fails
     */
    @Test
    public void testRewriteTwoArgumentFunction() throws Exception {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("document:match('BODY', 'car')");
        String rewritten = JexlStringBuildingVisitor.buildQueryWithoutParse(DocumentMatchFunctionRebuildingVisitor.rewrite(script));
        assertEquals("document:match('BODY', documentMatchContext, 'car')", rewritten);
    }
}
