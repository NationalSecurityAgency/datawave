package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;

public class RemoveExtraReferenceExpressionsVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    @BeforeEach
    public void setup() {
        // always validate node lineage
        validator.setValidateLineage(true);
    }

    @Test
    public void testLeafNodesNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "FOO == 'bar'",
                "FOO =~ 'ba.*'",
                "FOO != 'bar'",
                "FOO !~ 'bar'"
        };
        //  @formatter:on
        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testLeafNodesWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][] {
                // single reference expression
                {"(FOO == 'bar')", "FOO == 'bar'"},
                {"(FOO =~ 'ba.*')", "FOO =~ 'ba.*'"},
                {"(FOO != 'bar')", "FOO != 'bar'"},
                {"(FOO !~ 'bar')", "FOO !~ 'bar'"},
                // multiple reference expressions
                {"(((FOO == 'bar')))", "FOO == 'bar'"},
                {"(((FOO =~ 'ba.*')))", "FOO =~ 'ba.*'"},
                {"(((FOO != 'bar')))", "FOO != 'bar'"},
                {"(((FOO !~ 'bar')))", "FOO !~ 'bar'"}
        };
        //  @formatter:on
        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testUnionsNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "FOO == 'bar' || FOO == 'baz'",
                "(FOO =~ 'ba.*' || FOO == 'baz')",
                "FOO != 'bar' || FOO == 'baz'",
                "(FOO !~ 'bar' || FOO == 'baz')"
        };
        //  @formatter:on

        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testUnionsWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][] {
                // single reference expression
                {"(FOO == 'bar' || FOO == 'baz')", "(FOO == 'bar' || FOO == 'baz')"},
                {"((FOO == 'bar') || FOO == 'baz')", "(FOO == 'bar' || FOO == 'baz')"},
                {"(FOO == 'bar' || (FOO == 'baz'))", "(FOO == 'bar' || FOO == 'baz')"},
                {"((FOO == 'bar') || (FOO == 'baz'))", "(FOO == 'bar' || FOO == 'baz')"},
                {"(((FOO == 'bar' || FOO == 'baz')))", "(FOO == 'bar' || FOO == 'baz')"},
        };
        //  @formatter:on
        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testIntersectionsNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "FOO == 'bar' && FOO == 'baz'",
                "FOO =~ 'ba.*' && FOO == 'baz'",
                "FOO != 'bar' && FOO == 'baz'",
                "FOO !~ 'bar' && FOO == 'baz'"
        };
        //  @formatter:on

        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testIntersectionsWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][] {
                // single reference expression
                {"(FOO == 'bar' && FOO == 'baz')", "(FOO == 'bar' && FOO == 'baz')"},
                {"((FOO == 'bar') && FOO == 'baz')", "(FOO == 'bar' && FOO == 'baz')"},
                {"(FOO == 'bar' && (FOO == 'baz'))", "(FOO == 'bar' && FOO == 'baz')"},
                {"((FOO == 'bar') && (FOO == 'baz'))", "(FOO == 'bar' && FOO == 'baz')"},
                {"(((FOO == 'bar' && FOO == 'baz')))", "(FOO == 'bar' && FOO == 'baz')"},
        };
        //  @formatter:on
        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testNestedUnionsNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "F == 'a' && (F == 'b' || F == 'c')",
                "F == 'a' && (F == 'b' || F != 'c')",
                "F == 'a' && (!(F == 'b') || F == 'c')",
                "F =~ 'a' && (F == 'b' || F == 'c')",
        };
        //  @formatter:on

        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testNestedUnionsWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][]{
                {"F == 'a' && (F == 'b' || (F == 'c'))", "F == 'a' && (F == 'b' || F == 'c')"},
                {"F == 'a' && (((F == 'b' || F == 'c')))", "F == 'a' && (F == 'b' || F == 'c')"},
                {"F == 'a' && ((((F == 'b'))) || ((F == 'c')))", "F == 'a' && (F == 'b' || F == 'c')"},
                {"F == 'a' && (F == 'b' || F == 'c')", "F == 'a' && (F == 'b' || F == 'c')"},
        };
        //  @formatter:on

        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testNestedIntersectionsNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "F == 'a' || (F == 'b' && F == 'c')",
                "F == 'a' || (F == 'b' && F != 'c')",
                "F == 'a' || (!(F == 'b') && F == 'c')",
                "F =~ 'a' || (F == 'b' && F == 'c')",
        };
        //  @formatter:on

        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testNestedIntersectionsWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][]{
                {"F == 'a' || (F == 'b' && (F == 'c'))", "F == 'a' || (F == 'b' && F == 'c')"},
                {"F == 'a' || (((F == 'b' && F == 'c')))", "F == 'a' || (F == 'b' && F == 'c')"},
                {"F == 'a' || ((((F == 'b'))) && ((F == 'c')))", "F == 'a' || (F == 'b' && F == 'c')"},
                {"F == 'a' || (F == 'b' && F == 'c')", "F == 'a' || (F == 'b' && F == 'c')"},
        };
        //  @formatter:on

        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testNegationsNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "!(FOO == 'bar')",
                "!(FOO == 'bar' || FOO == 'baz')",
                "!((_Value_ = true) && (FOO =~ 'ba.*'))"
        };
        //  @formatter:on
        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testNegationsWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][] {
                {"!(((FOO == 'bar')))", "!(FOO == 'bar')"},
                {"!(((FOO == 'bar' || FOO == 'baz')))", "!(FOO == 'bar' || FOO == 'baz')"},
                {"!((((_Value_ = true) && (FOO =~ 'ba.*'))))", "!((_Value_ = true) && (FOO =~ 'ba.*'))"}
        };
        //  @formatter:on
        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    @Test
    public void testMarkersNoReduction() {
        //  @formatter:off
        String[] queries = new String[] {
                "((_Value_ = true) && (FOO =~ 'ba.*'))",
                "((_Delayed_ = true) && (FOO == 'bar'))",
                "((_Bounded_ = true) && (FOO >= '2' && FOO <= '3'))",
                "((_Delayed_ = true) && ((_Bounded_ = true) && (FOO >= '2' && FOO <= '3')))",
        };
        //  @formatter:on
        for (String query : queries) {
            testNoReduction(query);
        }
    }

    @Test
    public void testMarkersWithReduction() {
        //  @formatter:off
        String[][] pairs = new String[][] {
                {"((((_Value_ = true) && (FOO =~ 'ba.*'))))", "((_Value_ = true) && (FOO =~ 'ba.*'))"},
                {"((((_Delayed_ = true) && (FOO == 'bar'))))", "((_Delayed_ = true) && (FOO == 'bar'))"},
                {"((((_Bounded_ = true) && (FOO >= '2' && FOO <= '3'))))", "((_Bounded_ = true) && (FOO >= '2' && FOO <= '3'))"},
                {"((((_Delayed_ = true) && ((_Bounded_ = true) && (FOO >= '2' && FOO <= '3')))))", "((_Delayed_ = true) && ((_Bounded_ = true) && (FOO >= '2' && FOO <= '3')))"},
        };
        //  @formatter:on
        for (String[] pair : pairs) {
            testWithReduction(pair[0], pair[1]);
        }
    }

    /**
     * Entry point where no reduction is expected
     *
     * @param query
     *            the query
     */
    private void testNoReduction(String query) {
        test(query, query);
    }

    /**
     * Entry point where reduction is expected
     *
     * @param query
     *            the query
     * @param expected
     *            the expected result
     */
    private void testWithReduction(String query, String expected) {
        test(query, expected);
    }

    /**
     * Removes any extra reference expressions from the provided query, comparing the result against the expected query. If the input query and expected result
     * are equivalent, then no reduction is expected.
     *
     * @param query
     *            the query
     * @param expected
     *            the expected result
     */
    private void test(String query, String expected) {
        ASTJexlScript script = parse(query);
        validator.setValidateReferenceExpressions(query.equals(expected));
        validate(script);

        ASTJexlScript result = (ASTJexlScript) RemoveExtraReferenceExpressionsVisitor.remove(script);
        validator.setValidateLineage(true);
        validator.setValidateReferenceExpressions(true);
        validate(result);

        String resultString = JexlStringBuildingVisitor.buildQuery(result);
        assertEquals(resultString, expected);
    }

    /**
     * Parse the query string into a Jexl tree
     *
     * @param query
     *            the query
     * @return an {@link ASTJexlScript}
     */
    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate the query tree
     *
     * @param node
     *            the root of the query tree
     */
    private void validate(JexlNode node) {
        try {
            validator.isValid(node);
        } catch (InvalidQueryTreeException e) {
            fail("Query was invalid: " + node, e);
            throw new RuntimeException(e);
        }
    }
}
