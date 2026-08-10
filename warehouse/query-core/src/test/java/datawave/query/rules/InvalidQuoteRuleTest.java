package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InvalidQuoteRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query that does not contain any phrases with invalid quotes.
     */
    @Test
    void testQueryWithoutInvalidQuotes() throws Exception {
        givenQuery("FOO:'abc' OR FOO:'def'");
        // Do not expect to find any phrases.
        assertResult();
    }

    /**
     * Test a query that contains phrases with invalid quotes at both ends.
     */
    @Test
    void testQueryWithInvalidQuotesAtBothEndsOfPhrases() throws Exception {
        givenQuery("FOO:`abc` OR FOO:`def` OR FOO:'efg'");
        expectMessage("Invalid quote ` used in phrase \"FOO:`abc`\". Use ' instead.");
        expectMessage("Invalid quote ` used in phrase \"FOO:`def`\". Use ' instead.");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote at the start.
     */
    @Test
    void testQueryWithInvalidQuotesAtStartOPhrase() throws Exception {
        givenQuery("FOO:`abc' OR FOO:'efg'");
        expectMessage("Invalid quote ` used in phrase \"FOO:`abc'\". Use ' instead.");
        assertResult();
    }

    /**
     * Test a query that contains the invalid quote within the phrase, but not at either end.
     */
    @Test
    void testQueryWithEmptyInvalidQuotedInMiddle() throws Exception {
        givenQuery("FOO:'ab`cd' OR FOO:'efg'");
        // Do not expect to find any phrases.
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote at the end.
     */
    @Test
    void testQueryWithInvalidQuotesAtEndOPhrase() throws Exception {
        givenQuery("FOO:'abc` OR FOO:'efg'");
        expectMessage("Invalid quote ` used in phrase \"FOO:'abc`\". Use ' instead.");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an empty phrase with invalid quotes.
     */
    @Test
    void testQueryWithEmptyInvalidQuotedPhrase() throws Exception {
        givenQuery("FOO:`` OR FOO:'efg'");
        expectMessage("Invalid quote ` used in phrase \"FOO:``\". Use ' instead.");
        assertResult();
    }

    /**
     * Test a query that contains a phrase that is just one invalid quote.
     */
    @Test
    void testPhraseThatConsistsOfSingleInvalidQuote() throws Exception {
        givenQuery("FOO:` OR FOO:'efg'");
        expectMessage("Invalid quote ` used in phrase \"FOO:`\". Use ' instead.");
        assertResult();
    }

    /**
     * Test a query that contains a phrase with an invalid quote inside a function.
     */
    @Test
    void testFunctionWithInvalidQuote() throws Exception {
        givenQuery("FOO:'abc' AND #INCLUDE(BAR,`def`)");
        expectMessage("Invalid quote ` used in phrase \"#INCLUDE(BAR, `def`)\". Use ' instead.");
        assertResult();
    }

    /**
     * Test unfielded terms with invalid quotes.
     */
    @Test
    void testTermWithInvalidQuote() throws Exception {
        givenQuery("`def` `abc`");
        expectMessage("Invalid quote ` used in phrase \"`def`\". Use ' instead.");
        expectMessage("Invalid quote ` used in phrase \"`abc`\". Use ' instead.");
        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new InvalidQuoteRule(ruleName);
    }
}
