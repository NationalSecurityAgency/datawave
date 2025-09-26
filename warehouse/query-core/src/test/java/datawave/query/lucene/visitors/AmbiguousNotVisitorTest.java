package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

public class AmbiguousNotVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private String query;
    private final List<NotBooleanQueryNode> expectedNodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedNodes.clear();
    }

    /**
     * Test a query that does not contain a NOT.
     */
    @Test
    void testQueryWithoutNOT() throws QueryNodeParseException {
        givenQuery("FOO:123");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with a single unwrapped term preceding the NOT.
     */
    @Test
    void testNOTWithSingleUnwrappedPrecedingTerms() throws QueryNodeParseException {
        givenQuery("FIELD1:abc NOT FIELD:def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with a single wrapped term preceding the NOT.
     */
    @Test
    void testNOTWithSingleWrappedPrecedingTerms() throws QueryNodeParseException {
        givenQuery("(FIELD1:abc) NOT FIELD:def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with multiple wrapped terms preceding the NOT.
     */
    @ParameterizedTest()
    @ValueSource(strings = {"OR", "AND"})
    void testNOTWithWrappedMultiplePrecedingTerms(String junction) throws QueryNodeParseException {
        givenQuery("(FIELD1:abc " + junction + " FIELD2:def) NOT FIELD:ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a NOT with multiple unwrapped terms preceding the NOT.
     */
    @ParameterizedTest()
    @ValueSource(strings = {"OR", "AND"})
    void testNOTWithUnwrappedMultiplePrecedingTerms(String junction) throws QueryNodeParseException {
        givenQuery("FIELD1:abc " + junction + " FIELD2:def NOT FIELD:ghi");

        expectNode("FIELD1:abc " + junction + " FIELD2:def NOT FIELD:ghi");

        assertResult();
    }

    /**
     * Test a query with a NOT with multiple unwrapped terms preceding the NOT that will be automatically ANDed.
     *
     * @throws QueryNodeParseException
     */
    @Test
    void testNOTWithUnwrappedAutomaticallyAndedPreceedingTerms() throws QueryNodeParseException {
        givenQuery("FIELD1:abc FIELD2:def NOT FIELD:ghi");

        expectNode("FIELD1:abc FIELD2:def NOT FIELD:ghi");

        assertResult();
    }

    /**
     * Test a query with a NOT with multiple wrapped terms preceding the NOT that will be automatically ANDed.
     *
     * @throws QueryNodeParseException
     */
    @Test
    void testNOTWithWrappedAutomaticallyAndedPreceedingTerms() throws QueryNodeParseException {
        givenQuery("(FIELD1:abc FIELD2:def) NOT FIELD:ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query that does not consist entirely of a NOT.
     */
    @Test
    void testQueryWithTermThatIsNotPartOfNOT() throws QueryNodeParseException {
        givenQuery("FIELD1:abc OR (FIELD2:abc FIELD3:def NOT FIELD4:ghi)");

        expectNode("FIELD2:abc FIELD3:def NOT FIELD4:ghi");

        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectNode(String query) throws QueryNodeParseException {
        this.expectedNodes.add((NotBooleanQueryNode) parser.parse(query, ""));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode queryNode = parser.parse(query, "");
        List<NotBooleanQueryNode> actual = AmbiguousNotVisitor.check(queryNode);
        // Compare the node lists via their query strings.
        Assertions.assertThat(actual).usingElementComparator(QUERY_STR_COMPARATOR).isEqualTo(expectedNodes);
    }

    /**
     * A comparator implementation that will compare {@link NotBooleanQueryNode} based on their query strings.
     */
    private static final Comparator<NotBooleanQueryNode> QUERY_STR_COMPARATOR = new Comparator<NotBooleanQueryNode>() {

        private final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

        @Override
        public int compare(NotBooleanQueryNode first, NotBooleanQueryNode second) {
            if (first == second) {
                return 0;
            }
            if (first == null) {
                return -1;
            }
            if (second == null) {
                return 1;
            }
            return first.toQueryString(escapedSyntax).toString().compareTo(second.toQueryString(escapedSyntax).toString());
        }
    };
}
