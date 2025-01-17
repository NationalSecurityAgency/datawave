package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnfieldedTermsRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query without unfielded terms.
     */
    @Test
    void testQueryWithoutUnfieldedTerms() throws Exception {
        givenQuery("FOO:123 OR BAR:654");
        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test a query with unfielded terms.
     */
    @Test
    void testQueryWithUnfieldedTerms() throws Exception {
        givenQuery("FOO:123 643 OR abc 'bef'");

        expectMessage("Unfielded term 643 found.");
        expectMessage("Unfielded term abc found.");
        expectMessage("Unfielded term 'bef' found.");

        assertResult();
    }

    /**
     * Test that grouped terms directly after a field are not flagged.
     */
    @Test
    void testGroupedFieldTerms() throws Exception {
        givenQuery("643 OR FIELD:(123 OR 456)");

        expectMessage("Unfielded term 643 found.");

        assertResult();
    }

    @Test
    void testUnfieldedQuotedPhrases() {
        givenQuery("FOO:123 or \"abc\"");

        expectMessage("Unfielded term \"abc\" found.");
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new UnfieldedTermsRule(ruleName);
    }
}
