package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmbiguousGroupedUnquotedPhrasesRuleTest extends ShardQueryRuleTest {

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
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:abc AND def AND ghi. Recommended: FOO:\"abc def ghi\"");
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
     * Test a query with grouped ambiguous phrases before a fielded term.
     */
    @Test
    void testMultipleFieldsWithVariousGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:(abc def ghi) OR BAR:(aaa bbb ccc) AND 333 HAT:\"111\" 222 AND HEN:car VEE:(elephant zebra) VEE:deer");

        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:( abc AND def AND ghi ). Recommended: FOO:\"abc def ghi\"");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: BAR:( aaa AND bbb AND ccc ). Recommended: BAR:\"aaa bbb ccc\"");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: VEE:( elephant AND zebra ). Recommended: VEE:\"elephant zebra\"");

        assertResult();
    }

    /**
     * Test a query with grouped ambiguous terms after field
     */
    @Test
    void testGroupedAmbiguousPhrasesAfterField() throws Exception {
        givenQuery("FOO:(abc def ghi)");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:( abc AND def AND ghi ). Recommended: FOO:\"abc def ghi\"");

        assertResult();
    }

    /**
     * Test a query with grouped ambiguous terms does this really need a test...
     */
    @Test
    void testGroupedAmbiguousPhrases() throws Exception {
        givenQuery("(FOO:abc def ghi)");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: ( FOO:abc AND def AND ghi ). Recommended: FOO:\"abc def ghi\"");

        assertResult();
    }

    /**
     * Test a query with nested grouped ambiguous terms
     */
    @Test
    void testNestedFirstGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:((abc) def ghi)");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:def AND ghi. Recommended: FOO:\"def ghi\"");

        assertResult();
    }

    /**
     * Test a query with multiple nested grouped ambiguous terms
     */
    @Test
    void testNestedMultipleGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:(abc (def ghi) (jkl mno))");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:( def AND ghi ). Recommended: FOO:\"def ghi\"");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:( jkl AND mno ). Recommended: FOO:\"jkl mno\"");

        assertResult();
    }

    /**
     * Test a query with nested grouped ambiguous terms
     */
    @Test
    void testNestedLastGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:(abc def (ghi))");
        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:abc AND def. Recommended: FOO:\"abc def\"");

        assertResult();
    }

    /**
     * Test a query with one grouped ambiguous terms
     */
    @Test
    void testFirstGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:(abc) def ghi");

        assertResult();
    }

    /**
     * Test a query with nested grouped ambiguous terms following a fielded term.
     */
    @Test
    void testNestedGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:abc (def ghi (jkl))");

        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:abc AND ( def AND ghi AND ( jkl ) ). Recommended: FOO:\"abc def ghi jkl\"");

        assertResult();
    }

    /**
     * Test a query with ambiguous terms that are grouped and explicitly ANDed with a preceding fielded term.
     */
    @Test
    void testAmbiguousGroupedPhrasesAfterExplicitAND() throws Exception {
        givenQuery("FOO:(abc AND def AND ghi)");

        expectMessage("Ambiguous grouped unfielded terms AND'd with fielded term detected: FOO:( abc AND def AND ghi ). Recommended: FOO:\"abc def ghi\"");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new AmbiguousGroupedUnquotedPhrasesRule(ruleName);
    }
}
