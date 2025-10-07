package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmbiguousUnquotedPhrasesRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test query without ambiguous phrases.
     */
    @Test
    void testQueryWithoutAmbiguousPhrases() throws Exception {
        givenQuery("FOO:\"123 456\" OR FOO:bef");
        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with ambiguous phrases after an unquoted fielded term.
     */
    @Test
    void testAmbiguousPhraseAfterFieldedTerm() throws Exception {
        givenQuery("FOO:abc def ghi");
        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: FOO:abc AND def AND ghi. Recommended: FOO:\"abc def ghi\"");
        assertResult();
    }

    /**
     * Test a query with ambiguous phrases after a quoted phrase.
     */
    @Test
    void testAmbiguousPhraseAfterQuotedFieldedTerm() throws Exception {
        givenQuery("FOO:\"abc\" def ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with ambiguous phrases before a fielded term.
     */
    @Test
    void testAmbiguousPhraseBeforeFieldedTerm() throws Exception {
        givenQuery("abc def FOO:ghi");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with ambiguous phrases before a fielded term.
     */
    @Test
    void testMultipleFieldsWithAmbiguousPhrases() throws Exception {
        givenQuery("FOO:abc def ghi OR BAR:aaa bbb ccc AND 333 HAT:\"111\" 222 AND HEN:car VEE:elephant zebra VEE:deer");

        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: FOO:abc AND def AND ghi. Recommended: FOO:\"abc def ghi\"");
        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: BAR:aaa AND bbb AND ccc. Recommended: BAR:\"aaa bbb ccc\"");
        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: VEE:elephant AND zebra. Recommended: VEE:\"elephant zebra\"");

        assertResult();
    }

    /**
     * Test a query with grouped ambiguous terms following a fielded term.
     */
    @Test
    void testGroupedAmbiguousPhrasesAfterFieldedTerm() throws Exception {
        givenQuery("FOO:abc (def ghi)");

        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: FOO:abc AND ( def AND ghi ). Recommended: FOO:\"abc def ghi\"");

        assertResult();
    }

    /**
     * Test a query with nested grouped ambiguous terms following a fielded term.
     */
    @Test
    void testNestedGroupedAmbiguousPhrasesAfterFieldedTerm() throws Exception {
        givenQuery("FOO:abc (def ghi (jkl))");

        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: FOO:abc AND ( def AND ghi AND ( jkl ) ). Recommended: FOO:\"abc def ghi jkl\"");

        assertResult();
    }

    /**
     * Test a query with ambiguous terms that are explicitly ANDed with a preceding fielded term.
     */
    @Test
    void testAmbiguousPhrasesAfterExplicitANDWithFieldedTerm() throws Exception {
        givenQuery("FOO:abc AND def AND ghi");

        expectMessage("Ambiguous unfielded terms AND'd with fielded term detected: FOO:abc AND def AND ghi. Recommended: FOO:\"abc def ghi\"");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new AmbiguousUnquotedPhrasesRule(ruleName);
    }
}
