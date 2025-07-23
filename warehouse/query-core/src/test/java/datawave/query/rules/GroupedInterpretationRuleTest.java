package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GroupedInterpretationRuleTest extends ShardQueryRuleTest {

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

    @Test
    void testQueryWithGroupedAmbiguousPhrases() throws Exception {
        givenQuery("FOO:(abc def ghi)");

        expectMessage("null will be interpreted as: ( FOO:abc AND FOO:def AND FOO:ghi )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases1() throws Exception {
        givenQuery("(FOO:abc def ghi)");

        expectMessage("null will be interpreted as: ( FOO:abc AND def AND ghi )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases2() throws Exception {
        givenQuery("(FOO:abc AND FOO:def AND FOO:ghi)");

        expectMessage("null will be interpreted as: ( FOO:abc AND FOO:def AND FOO:ghi )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases3() throws Exception {
        givenQuery("FOO:(abc (def ghi))");

        expectMessage("null will be interpreted as: ( FOO:abc AND ( FOO:def AND FOO:ghi ) )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases4() throws Exception {
        givenQuery("FOO:(abc def ghi) FOO:jkl mno");

        expectMessage("null will be interpreted as: ( FOO:abc AND FOO:def AND FOO:ghi )");

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
        givenQuery("FOO:abc def ghi OR BAR:aaa bbb ccc AND 333 HAT:\"111\" 222 AND HEN:car VEE:elephant zebra VEE:deer FOO:(aaa bbb ccc)");

        expectMessage("null will be interpreted as: ( FOO:aaa AND FOO:bbb AND FOO:ccc )");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new GroupedInterpretationRule(ruleName);
    }
}
