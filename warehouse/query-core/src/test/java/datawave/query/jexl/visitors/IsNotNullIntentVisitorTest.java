package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class IsNotNullIntentVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    @Test
    public void testMatchAnythingRegex() throws ParseException {
        String query = "FOO =~ '.*?'";
        String expected = "FOO != null";

        assertResult(query, expected);
    }

    @Test
    public void testMatchSpecificValue() throws ParseException {
        String query = "FOO =~ 'value*'";
        String expected = "FOO =~ 'value*'";

        assertResult(query, expected);
    }

    @Test
    public void testConjunctionWithMatchAnythingRegex() throws ParseException {
        String query = "FOO =~ '.*?' && BAR =~ 'anything*'";
        String expected = "FOO != null && BAR =~ 'anything*'";

        assertResult(query, expected);
    }

    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseAndFlattenJexlQuery(original);
        ASTJexlScript actual = IsNotNullIntentVisitor.fixNotNullIntent(originalScript);

        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();

        try {
            validator.isValid(actual);
        } catch (InvalidQueryTreeException e) {
            Assert.fail("IsNotNullIntentVisitor produced an invalid query tree: " + e.getMessage());
        }
    }
}
