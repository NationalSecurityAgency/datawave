package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;

class UnescapedWildcardsInQuotedPhrasesVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private String query;
    private final List<Pair<String,String>> expectedPhrases = new ArrayList<>();

    @AfterEach
    void tearDown() {
        query = null;
        expectedPhrases.clear();
    }

    /**
     * Test a query with a quoted phrase without wildcards.
     */
    @Test
    void testQuotedPhraseWithoutWildcards() throws QueryNodeParseException {
        givenQuery("FOO:\"abc\"");
        // Do not expect any phrases.
        assertResult();
    }

    /**
     * Test a query with an quoted phrase an escaped wildcard.
     */
    @Test
    void testQuotedPhraseWithEscapedWildcard() throws QueryNodeParseException {
        // Backslash must be escaped here for it to remain in parsed query.
        givenQuery("FOO:\"a\\\\*bc\"");
        // Do not expect any phrases.
        assertResult();
    }

    /**
     * Test a query with quoted phrases with a non-escaped wildcard at the beginning, in the middle, and at the end of the phrase.
     */
    @Test
    void testQuotedPhraseWithUnescapedWildcard() throws QueryNodeParseException {
        givenQuery("FOO:\"*abc\" OR FOO:\"de*f\" OR FOO:\"efg*\"");
        expectFieldedPhrase("FOO", "*abc");
        expectFieldedPhrase("FOO", "de*f");
        expectFieldedPhrase("FOO", "efg*");
        assertResult();
    }

    /**
     * Test a query with an unfielded quoted phrases with a non-escaped wildcard.
     */
    @Test
    void testUnfieldedQuotedPhraseWithUnescapedWildcard() throws QueryNodeParseException {
        givenQuery("\"*abc\"");
        expectUnfieldedPhrase("*abc");
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectUnfieldedPhrase(String phrase) {
        this.expectedPhrases.add(Pair.of("", phrase));
    }

    private void expectFieldedPhrase(String field, String phrase) {
        this.expectedPhrases.add(Pair.of(field, phrase));
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode node = parser.parse(query, "");
        PrintingVisitor.printToStdOut(node);
        // @formatter:off
        List<Pair<String, String>> actual = UnescapedWildcardsInQuotedPhrasesVisitor.check(node).stream()
                        .map(result -> Pair.of(result.getFieldAsString(), result.getTextAsString()))
                        .collect(Collectors.toList());
        // @formatter:on

        Assertions.assertEquals(expectedPhrases, actual);
    }
}
