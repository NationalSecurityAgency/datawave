package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnescapedWildcardsInPhrasesRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query with a quoted phrase without wildcards.
     */
    @Test
    void testQuotedPhraseWithoutWildcards() throws Exception {
        givenQuery("FOO:\"abc\"");
        // Do not expect any phrases.
        assertResult();
    }

    /**
     * Test a query with an quoted phrase an escaped wildcard.
     */
    @Test
    void testQuotedPhraseWithEscapedWildcard() throws Exception {
        // Backslash must be escaped here for it to remain in parsed query.
        givenQuery("FOO:\"a\\\\*bc\"");
        // Do not expect any phrases.
        assertResult();
    }

    /**
     * Test a query with quoted phrases with a non-escaped wildcard at the beginning, in the middle, and at the end of the phrase.
     */
    @Test
    void testQuotedPhraseWithUnescapedWildcard() throws Exception {
        givenQuery("FOO:\"*abc\" OR FOO:\"de*f\" OR FOO:\"efg*\"");
        expectMessage("Wildcard found in phrase FOO:\"*abc\". Wildcards are not allowed in phrases so it will be searched as a literal character.");
        expectMessage("Wildcard found in phrase FOO:\"de*f\". Wildcards are not allowed in phrases so it will be searched as a literal character.");
        expectMessage("Wildcard found in phrase FOO:\"efg*\". Wildcards are not allowed in phrases so it will be searched as a literal character.");
        assertResult();
    }

    /**
     * Test a query with an unfielded quoted phrases with a non-escaped wildcard.
     */
    @Test
    void testUnfieldedQuotedPhraseWithUnescapedWildcard() throws Exception {
        givenQuery("\"*abc\"");
        expectMessage("Wildcard found in phrase \"*abc\". Wildcards are not allowed in phrases so it will be searched as a literal character.");
        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new UnescapedWildcardsInPhrasesRule(ruleName);
    }
}
