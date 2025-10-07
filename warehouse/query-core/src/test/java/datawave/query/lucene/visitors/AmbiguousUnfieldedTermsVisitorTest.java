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
     * Test a query with quoted phrases. Only unquoted unfielded terms are expected.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testQueryWithQuotedPhrases(String junction) throws QueryNodeParseException {
        givenQuery("FOO:\"abc\" " + junction + " \"def\"");
        givenJunction(junction);

        // Do not expect any results.
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

    /**
     * Test a query with a variety of ambiguous phrases.
     */
    @ParameterizedTest
    @ValueSource(strings = {"OR", "AND"})
    void testMixedComplexityQuery(String junction) throws QueryNodeParseException {
        String otherJunction = junction.equals("OR") ? "AND" : "OR";
        givenQuery("FOO:aaa " + otherJunction + " bbb " + otherJunction + " (BAR:aaa " + junction + " bbb " + junction + " ccc " + junction
                        + " HAT:\"ear\" nose) " + junction + " (aaa " + junction + " bbb " + junction + " VEE:eee " + junction + " 123 " + junction + " (gee "
                        + junction + " \"wiz\")) " + otherJunction + " (EGG:yolk " + junction + " shell)");
        givenJunction(junction);

        expectNode("BAR:aaa " + junction + " bbb " + junction + " ccc");
        expectNode("VEE:eee " + junction + " 123");
        expectNode("(EGG:yolk " + junction + " shell)");

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
        Assertions.assertEquals(expectedStrs, actualStrs);
    }
}
