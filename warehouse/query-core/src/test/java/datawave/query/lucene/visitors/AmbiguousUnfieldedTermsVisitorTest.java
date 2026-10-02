package datawave.query.lucene.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

class AmbiguousUnfieldedTermsVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();
    private static final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

    private String query;
    private AmbiguousUnfieldedTermsVisitor.JUNCTION junction;
    private final List<QueryNode> expectedNodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        junction = null;
        expectedNodes.clear();
    }

    /**
     * Test a query with a single fielded term.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithSingleFieldedTerm(String junction) throws QueryNodeParseException {
        givenQuery("FOO:abc");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a single fielded term.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithWrappedSingleFieldedTerm(String junction) throws QueryNodeParseException {
        givenQuery("(FOO:abc)");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that consists of a single unfielded terms. Only unfielded terms directly following a fielded term are expected.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithUnfieldedTermOnly(String junction) throws QueryNodeParseException {
        givenQuery("abc");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that consists of an unfielded term before a fielded term. Only unfielded terms directly following a fielded term are expected.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithUnfieldedTermBeforeFieldedTerm(String junction) throws QueryNodeParseException {
        givenQuery("abc FOO:def");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with quoted phrases.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithQuotedPhrases(String junction) throws QueryNodeParseException {
        givenQuery("FOO:\"abc\" " + junction + " \"def\"");
        givenJunction(junction);

        expectNode("FOO:\"abc\" " + junction + "\"def\"");

        assertResult();
    }

    /**
     * Test a query with quoted and unquoted phrases.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithQuotedAndUnquotedPhrases(String junction) throws QueryNodeParseException {
        givenQuery("FOO:\"abc\" " + junction + " \"def\" " + junction + " ghi");
        givenJunction(junction);

        expectNode("FOO:\"abc\" " + junction + "\"def\" " + junction + " ghi");

        assertResult();
    }

    /**
     * Test a query where terms are wrapped directly after a field name.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithWrappedTerms(String junction) throws QueryNodeParseException {
        givenQuery("FOO:(abc " + junction + " def)");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where terms are wrapped multiple times in a nested fashion.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithNestedWrappedTerms(String junction) throws QueryNodeParseException {
        givenQuery("FOO:(((abc " + junction + " def)))");
        givenJunction(junction);

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where a single unfielded term follows a fielded term.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithSingleUnfieldedTermAfterFieldedTerm(String junction) throws QueryNodeParseException {
        givenQuery("FOO:abc " + junction + " def");
        givenJunction(junction);

        // Expect the terms.
        expectNode("FOO:abc " + junction + " def");

        assertResult();
    }

    /**
     * Test a query with a fielded term and a non-fielded term that are implied to be AND'd, with the target junction AND.
     */
    @Test
    void testQueryWithImpliedAndGivenJunctionAND() throws QueryNodeParseException {
        givenQuery("FOO:abc def");
        givenJunction("AND");

        expectNode("FOO:abc def");

        assertResult();
    }

    /**
     * Test a query with a fielded term and a non-fielded term that are implied to be AND'd, with the target junction OR.
     */
    @Test
    void testQueryWithImpliedAndGivenJunctionOR() throws QueryNodeParseException {
        givenQuery("FOO:abc def");
        givenJunction("OR");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where multiple unfielded terms follows a fielded term.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithMultipleUnfieldedTermAfterFieldedTerm(String junction) throws QueryNodeParseException {
        givenQuery("FOO:abc " + junction + " def " + junction + " efg");
        givenJunction(junction);

        // Expect the terms.
        expectNode("FOO:abc " + junction + " def " + junction + " efg");

        assertResult();
    }

    /**
     * Test a query where multiple unfielded terms follows a fielded term are all grouped.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithFullyGroupedFieldedTermAndUnfieldedTerms(String junction) throws QueryNodeParseException {
        givenQuery("(FOO:abc " + junction + " def " + junction + " efg)");
        givenJunction(junction);

        // Expect the terms.
        expectNode("(FOO:abc " + junction + " def " + junction + " efg)");

        assertResult();
    }

    /**
     * Test a query with unfielded terms nested within multiple groups.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithNestedUnfieldedTerms(String junction) throws QueryNodeParseException {
        givenQuery("(FOO:abc " + junction + " (def " + junction + " efg " + junction + "(jkl)))");
        givenJunction(junction);

        // Expect the terms.
        expectNode("(FOO:abc " + junction + " (def " + junction + " efg " + junction + "(jkl)))");

        assertResult();
    }

    /**
     * Test a query where multiple grouped unfielded terms follows a fielded term.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithFieldedTermAndGroupedUnfieldedTerms(String junction) throws QueryNodeParseException {
        givenQuery("FOO:abc " + junction + " (def " + junction + " efg)");
        givenJunction(junction);

        // Expect the terms.
        expectNode("FOO:abc " + junction + " (def " + junction + " efg)");

        assertResult();
    }

    /**
     * Test a query where the fielded term is in a sibling group.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithFieldedTermInSiblingGroup(String junction) throws QueryNodeParseException {
        givenQuery("((FOO:abc " + junction + " def) " + junction + " (aaa " + junction + " bbb))");
        givenJunction(junction);

        // Only expect the terms from the first group sibling.
        expectNode("(FOO:abc " + junction + " def)");

        assertResult();
    }

    /**
     * Test a query with multiple sets of ambiguous phrases.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithMultipleAmbiguousPhrases(String junction) throws QueryNodeParseException {
        givenQuery("FOO:abc " + junction + " def " + junction + " BAR:aaa " + junction + " bbb");
        givenJunction(junction);

        expectNode("FOO:abc " + junction + " def");
        expectNode("BAR:aaa " + junction + " bbb");

        assertResult();
    }

    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testConsecutiveAmbiguousPhrasesWithDifferingFields(String junction) throws QueryNodeParseException {
        givenQuery("FOO:aaa " + junction + " \"def\" " + junction + " HAT:\"ear\" " + junction + " nose");
        givenJunction(junction);

        expectNode("FOO:aaa " + junction + " \"def\"");
        expectNode("HAT:\"ear\" " + junction + " nose");

        assertResult();
    }

    /**
     * Test a query with a variety of ambiguous phrases with the target junction AND.
     */
    @Test
    void testMixedComplexityQueryWithTargetJunctionAND() throws QueryNodeParseException {
        givenQuery("FOO:aaa OR bbb OR (BAR:aaa AND bbb AND ccc AND HAT:\"ear\" nose) AND (aaa AND bbb AND VEE:eee AND 123 AND (gee AND \"wiz\")) OR (EGG:yolk AND shell)");
        givenJunction("AND");

        expectNode("BAR:aaa AND bbb AND ccc");
        expectNode("HAT:\"ear\" AND nose");
        expectNode("VEE:eee AND 123 AND (gee AND \"wiz\")");
        expectNode("(EGG:yolk AND shell)");

        assertResult();
    }

    /**
     * Test a query with a variety of ambiguous phrases with the target junction OR.
     */
    @Test
    void testMixedComplexityQueryWithTargetJunctionOR() throws QueryNodeParseException {
        givenQuery("FOO:aaa AND bbb AND (BAR:aaa OR bbb OR ccc OR HAT:\"ear\" nose) OR (aaa OR bbb OR VEE:eee OR 123 OR (gee OR \"wiz\")) AND (EGG:yolk OR shell)");
        givenJunction("OR");

        expectNode("BAR:aaa OR bbb OR ccc");
        expectNode("VEE:eee OR 123 OR (gee OR \"wiz\")");
        expectNode("(EGG:yolk OR shell)");

        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenJunction(String junction) {
        this.junction = AmbiguousUnfieldedTermsVisitor.JUNCTION.valueOf(junction);
    }

    private void expectNode(String node) throws QueryNodeParseException {
        expectedNodes.add(parser.parse(node, ""));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode queryNode = parser.parse(query, "");
        List<QueryNode> actual = AmbiguousUnfieldedTermsVisitor.check(queryNode, junction);
        // Compare the lists via their query strings.
        List<String> actualStrs = actual.stream().map(node -> node.toQueryString(escapedSyntax).toString()).collect(Collectors.toList());
        List<String> expectedStrs = expectedNodes.stream().map(node -> node.toQueryString(escapedSyntax).toString()).collect(Collectors.toList());
        assertEquals(expectedStrs, actualStrs);
    }
}
