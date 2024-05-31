package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.language.analyzers.LanguageAnalyzer;
import datawave.query.language.analyzers.lucene.EnglishLuceneAnalyzer;
import datawave.query.language.analyzers.lucene.LanguageAwareAnalyzer;
import datawave.query.language.analyzers.lucene.SpanishLuceneAnalyzer;

class LanguageExpansionVisitorTest {

    private static final Logger log = LoggerFactory.getLogger(LanguageExpansionVisitorTest.class);

    private LanguageExpansionVisitor visitor;

    private final ASTValidator validator = new ASTValidator();

    @BeforeEach
    public void beforeEach() {
        List<LanguageAwareAnalyzer> analyzers = List.of(new EnglishLuceneAnalyzer());

        Set<String> tokenizedFields = Set.of("TOK");

        visitor = new LanguageExpansionVisitor(analyzers, tokenizedFields);

        validator.setValidateFlatten(true);
        validator.setValidateJunctions(true);
        validator.setValidateLineage(true);
        validator.setValidateReferenceExpressions(true);
        validator.setValidateQueryPropertyMarkers(true);
    }

    @Test
    public void testSingleTermExpansion() {
        //  @formatter:off
        String[][] inputs = {
                {"TOK == 'boys'", "TOK == 'boys' || TOK == 'boi'"},
                {"TOK == 'dogs'", "TOK == 'dog' || TOK == 'dogs'"},
                {"TOK == 'people'", "TOK == 'peopl' || TOK == 'people'"},
                {"TOK == 'travelling'", "TOK == 'travel' || TOK == 'travelling'"},
                {"TOK == 'romanticize'", "TOK == 'romantic' || TOK == 'romanticize'"}};
        //  @formatter:on
        test(inputs);
    }

    // Edge Cases

    @Test
    public void testExpansionOfChildWithinIntersection() {
        String[][] inputs = {
                // expand left term
                {"TOK == 'boys' && TOK == 'tacocat'", "(TOK == 'boi' || TOK == 'boys') && TOK == 'tacocat'"},
                {"TOK == 'history' && TOK == 'class'", "(TOK == 'histori' || TOK == 'history') && TOK == 'class'"},
                // expand right term
                {"TOK == 'poet' && TOK == 'romanticize'", "TOK == 'poet' && (TOK == 'romantic' || TOK == 'romanticize')"},
                // expand both terms
                {"TOK == 'cats' && TOK == 'dogs'", "(TOK == 'cat' || TOK == 'cats') && (TOK == 'dog' || TOK == 'dogs')"},
                {"TOK == 'people' && TOK == 'watching'", "(TOK == 'peopl' || TOK == 'people') && (TOK == 'watch' || TOK == 'watching')"},
                // expand no terms
                {"TOK == 'dog' && TOK == 'cat'", "TOK == 'dog' && TOK == 'cat'"}};
        test(inputs);
    }

    @Test
    public void testExpansionOfChildWithinUnion() {
        // should seamlessly add children to existing parent union
        String[][] inputs = {
                // expand left term
                {"TOK == 'boys' || TOK == 'tacocat'", "TOK == 'boi' || TOK == 'boys' || TOK == 'tacocat'"},
                {"TOK == 'history' || TOK == 'class'", "TOK == 'histori' || TOK == 'history' || TOK == 'class'"},
                // expand right term
                {"TOK == 'poet' || TOK == 'romanticize'", "TOK == 'poet' || TOK == 'romantic' || TOK == 'romanticize'"},
                // expand both terms
                {"TOK == 'cats' || TOK == 'dogs'", "TOK == 'cat' || TOK == 'cats' || TOK == 'dog' || TOK == 'dogs'"},
                {"TOK == 'people' || TOK == 'watching'", "TOK == 'peopl' || TOK == 'people' || TOK == 'watch' || TOK == 'watching'"},
                // expand no terms
                {"TOK == 'dog' || TOK == 'cat'", "TOK == 'dog' || TOK == 'cat'"}};
        test(inputs);
    }

    @Test
    public void testExpansionOfDelayedTerm() {
        // should expand child without destroying marker node
        String query = "((_Delayed_ = true) && (TOK == 'boys'))";
        String expected = "((_Delayed_ = true) && (TOK == 'boi' || TOK == 'boys'))";
        test(query, expected);
    }

    @Test
    public void testExpansionOfSingleNodeNullParent() {
        // nodes without a parent cannot be expanded
        JexlNode node = JexlNodeFactory.buildEQNode("TOK", "cats");
        JexlNode expanded = visitor.expand(node);

        assertTrue(TreeEqualityVisitor.isEqual(node, expanded));
        assertSame(node, expanded);
    }

    @Disabled
    @Test
    public void testMarkerNodes() {
        fail("not yet implemented");
    }

    @Test
    public void testNullLiterals() {
        // null literals should not be processed
        String query = "TOK == 'null'";
        test(query, query);
    }

    @Test
    public void testArithmetic() {
        // arithmetic queries should remain unchanged
        String query = "TOK.min() == TOK.max()";
        test(query, query);
    }

    @Test
    public void testMultiLanguageQuery() {
        List<LanguageAwareAnalyzer> analyzers = List.of(new EnglishLuceneAnalyzer(), new SpanishLuceneAnalyzer());
        visitor = new LanguageExpansionVisitor(analyzers, Set.of("TOK"));

        String query = "TOK == 'boys' && TOK == 'chicas'";
        String expected = "(TOK == 'boi' || TOK == 'boys') && (TOK == 'chica' || TOK == 'chic' || TOK == 'chicas')";
        test(query, expected);
    }

    /**
     * Wrapper that enabled parameterization
     *
     * @param inputs
     *            an array of expected inputs and outputs
     */
    private void test(String[][] inputs) {
        for (String[] input : inputs) {
            test(input[0], input[1]);
        }
    }

    /**
     * Apply all language analyzers to the query and assert the output matches the expectation
     *
     * @param query
     *            the query
     * @param expected
     *            the expected transformation
     */
    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);

            ASTJexlScript visited = (ASTJexlScript) visitor.expand(script);

            // assert equality
            if (!TreeEqualityVisitor.isEqual(expectedScript, visited)) {
                log.error("queries were not equal");
                log.error("expected: {}", expected);
                log.error(" visited: {}", JexlStringBuildingVisitor.buildQuery(visited));
                fail("queries were not equal after visit");
            }

            // assert tree validity
            assertTrue(validator.isValid(visited));
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        } catch (InvalidQueryTreeException e) {
            fail("Failed to validate resulting query");
        }
    }
}
