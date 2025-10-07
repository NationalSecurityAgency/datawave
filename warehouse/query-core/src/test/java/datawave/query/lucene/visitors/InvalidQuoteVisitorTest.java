package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;

public class InvalidQuoteVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private String query;
    private final List<QueryNode> expected = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        expected.clear();
    }

    /**
     * Test a query that does not contain any phrases with invalid quotes.
     */
    @Test
    void testQueryWithoutInvalidQuotes() throws QueryNodeParseException {
        givenQuery("FOO:'abc' OR FOO:'def'");
        // Do not expect to find any phrases.
        assertResult();
    }

    /**
     * Test a query that contains phrases with invalid quotes at both ends.
     */
    @Test
    void testQueryWithInvalidQuotesAtBothEndsOfPhrases() throws QueryNodeParseException {
        givenQuery("FOO:`abc` OR FOO:`def` OR FOO:'efg'");
        expect("FOO:`abc`");
        expect("FOO:`def`");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote at the start.
     */
    @Test
    void testQueryWithInvalidQuotesAtStartOPhrase() throws QueryNodeParseException {
        givenQuery("FOO:`abc' OR FOO:'efg'");
        expect("FOO:`abc'");
        assertResult();
    }

    /**
     * Test a query that contains the invalid quote within the phrase, but not at either end.
     */
    @Test
    void testQueryWithEmptyInvalidQuotedInMiddle() throws QueryNodeParseException {
        givenQuery("FOO:'ab`cd' OR FOO:'efg'");
        // Do not expect to find any phrases.
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote at the end.
     */
    @Test
    void testQueryWithInvalidQuotesAtEndOPhrase() throws QueryNodeParseException {
        givenQuery("FOO:'abc` OR FOO:'efg'");
        expect("FOO:'abc`");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an empty phrase with invalid quotes.
     */
    @Test
    void testQueryWithEmptyInvalidQuotedPhrase() throws QueryNodeParseException {
        givenQuery("FOO:`` OR FOO:'efg'");
        expect("FOO:``");
        assertResult();
    }

    /**
     * Test a query that contains a phrase that is just one invalid quote.
     */
    @Test
    void testPhraseThatConsistsOfSingleInvalidQuote() throws QueryNodeParseException {
        givenQuery("FOO:` OR FOO:'efg'");
        expect("FOO:`");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote inside a function.
     */
    @Test
    void testFunctionWithInvalidQuote() throws QueryNodeParseException {
        givenQuery("FOO:'abc' AND #INCLUDE(BAR,`def`)");
        expect("#INCLUDE(BAR,`def`)");
        assertResult();
    }

    /**
     * Test unfielded terms with invalid quotes.
     */
    @Test
    void testTermWithInvalidQuote() throws QueryNodeParseException {
        givenQuery("`def` `abc`");
        expect("`def`");
        expect("`abc`");
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expect(String phrase) throws QueryNodeParseException {
        expected.add(parser.parse(phrase, ""));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode node = parser.parse(query, "");
        List<QueryNode> actual = InvalidQuoteVisitor.check(node);
        List<String> actualStrings = actual.stream().map(LuceneQueryStringBuildingVisitor::build).collect(Collectors.toList());
        List<String> expectedStrings = expected.stream().map(LuceneQueryStringBuildingVisitor::build).collect(Collectors.toList());
        // Compare the query strings.
        Assertions.assertEquals(expectedStrings, actualStrings);
    }
}
