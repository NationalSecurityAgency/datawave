package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;

class UnfieldedTermsVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private String query;
    private final List<String> expectedTerms = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedTerms.clear();
    }

    /**
     * Test a query without unfielded terms.
     */
    @Test
    void testQueryWithoutUnfieldedTerms() throws QueryNodeParseException {
        givenQuery("FOO:123 OR BAR:654");
        // Do not expect any terms.
        assertResult();
    }

    /**
     * Test a query with unfielded terms.
     */
    @Test
    void testQueryWithUnfieldedTerms() throws QueryNodeParseException {
        givenQuery("FOO:123 643 OR abc 'bef'");
        expectTerms("643", "abc", "'bef'");
        assertResult();
    }

    /**
     * Test that grouped terms directly after a field are not flagged.
     */
    @Test
    void testGroupedFieldTerms() throws QueryNodeParseException {
        givenQuery("643 OR FIELD:(123 OR 456)");
        expectTerms("643");
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectTerms(String... terms) {
        expectedTerms.addAll(List.of(terms));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode node = parser.parse(query, "");
        List<String> actual = UnfieldedTermsVisitor.check(node);
        Assertions.assertEquals(expectedTerms, actual);
    }
}
