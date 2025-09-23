package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

class GroupedInterpretationVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();
    private static final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

    private String query;
    private final List<QueryNode> expectedNodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedNodes.clear();
    }

    /**
     * Test a query with a single fielded term.
     */
    @Test
    void testQueryWithSingleFieldedTerm() throws QueryNodeParseException {
        givenQuery("FOO:abc");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a single fielded term.
     */
    @Test
    void testQueryWithWrappedSingleFieldedTerm() throws QueryNodeParseException {
        givenQuery("(FOO:abc)");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that consists of a single unfielded terms. Only unfielded terms directly following a fielded term are expected.
     */
    @Test
    void testQueryWithUnfieldedTermOnly() throws QueryNodeParseException {
        givenQuery("abc");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that consists of an unfielded term before a fielded term. Only unfielded terms directly following a fielded term are expected.
     */
    @Test
    void testQueryWithUnfieldedTermBeforeFieldedTerm() throws QueryNodeParseException {
        givenQuery("abc FOO:def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where terms are wrapped directly after a field name.
     */
    @Test
    void testQueryWithWrappedTerms() throws QueryNodeParseException {
        givenQuery("FOO:(abc AND def)");

        // Expect the terms.
        expectNode("(FOO:abc AND FOO:def)");

        assertResult();
    }

    /**
     * Test a query where terms are wrapped multiple times in a nested fashion.
     */
    @Test
    void testQueryWithNestedWrappedTerms() throws QueryNodeParseException {
        givenQuery("FOO:(((abc AND def)))");

        // Expect the terms.
        expectNode("(((FOO:abc AND FOO:def)))");

        assertResult();
    }

    /**
     * Test a query where a single unfielded term follows a fielded term.
     */
    @Test
    void testQueryWithSingleUnfieldedTermAfterFieldedTerm() throws QueryNodeParseException {
        givenQuery("FOO:abc AND def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where multiple unfielded terms follows a fielded term.
     */
    @Test
    void testQueryWithMultipleUnfieldedTermAfterFieldedTerm() throws QueryNodeParseException {
        givenQuery("FOO:abc AND def AND efg");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where multiple unfielded terms follows a fielded term are all grouped.
     */
    @Test
    void testQueryWithFullyGroupedFieldedTermAndUnfieldedTerms() throws QueryNodeParseException {
        givenQuery("(FOO:abc AND def AND efg)");

        // Expect the terms.
        expectNode("(FOO:abc AND def AND efg)");

        assertResult();
    }

    /**
     * Test a query with unfielded terms nested within multiple groups.
     */
    @Test
    void testQueryWithNestedUnfieldedTerms() throws QueryNodeParseException {
        givenQuery("(FOO:abc AND (def AND efg AND (jkl)))");

        // Expect the terms.
        expectNode("(FOO:abc AND (def AND efg AND (jkl)))");

        assertResult();
    }

    /**
     * Test a query where multiple grouped unfielded terms follows a fielded term.
     */
    @Test
    void testQueryWithFieldedTermAndGroupedUnfieldedTerms() throws QueryNodeParseException {
        givenQuery("FOO:abc AND (def AND efg)");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where the fielded term is in a sibling group.
     */
    @Test
    void testQueryWithFieldedTermInSiblingGroup() throws QueryNodeParseException {
        givenQuery("((FOO:abc AND def) AND (aaa AND bbb))");

        // Only expect the terms from the first group sibling.
        expectNode("(FOO:abc AND def)");

        assertResult();
    }

    /**
     * Test a query with multiple sets of ambiguous phrases.
     */
    @Test
    void testQueryWithMultipleGroupedAmbiguousPhrases() throws QueryNodeParseException {
        givenQuery("FOO:(abc AND def) AND (BAR:aaa AND bbb)");

        expectNode("(FOO:abc AND FOO:def)");
        expectNode("(BAR:aaa AND bbb)");

        assertResult();
    }

    /**
     * Test a query with a variety of ambiguous phrases.
     */
    @Test
    void testMixedComplexityQuery() throws QueryNodeParseException {
        givenQuery("FOO:aaa OR bbb OR (BAR:aaa AND bbb AND ccc AND HAT:\"ear\" nose) AND (aaa AND bbb AND VEE:eee AND 123 AND (gee AND \"wiz\")) OR (EGG:yolk AND shell)");

        expectNode("(BAR:aaa AND bbb AND ccc AND (HAT:\"ear\" AND nose)) ");
        expectNode("(EGG:yolk AND shell)");

        assertResult();
    }

    @Test
    void testGroupedUnfieldedTermWithNonFieldedClause() throws QueryNodeParseException {
        givenQuery("(FOO:abc def #ISNOTNULL(HAT))");

        expectNode("(FOO:abc def #ISNOTNULL(HAT))");

        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectNode(String node) throws QueryNodeParseException {
        expectedNodes.add(parser.parse(node, ""));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode queryNode = parser.parse(query, "");
        List<QueryNode> actual = GroupedInterpretationVisitor.check(queryNode);
        // Compare the lists via their query strings.
        List<String> actualStrs = actual.stream().map(node -> node.toQueryString(escapedSyntax).toString()).collect(Collectors.toList());
        List<String> expectedStrs = expectedNodes.stream().map(node -> node.toQueryString(escapedSyntax).toString()).collect(Collectors.toList());
        Assertions.assertEquals(expectedStrs, actualStrs);
    }
}
