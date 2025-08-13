package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GroupedInterpretationRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    @Test
    void testQueryWithGroupedPhrase() throws Exception {
        givenQuery("FOO:(abc def ghi)");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND FOO:def AND FOO:ghi )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases() throws Exception {
        givenQuery("(FOO:abc def ghi)");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND def AND ghi )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedFieldedPhrases() throws Exception {
        givenQuery("(FOO:abc AND FOO:def AND FOO:ghi)");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND FOO:def AND FOO:ghi )");

        assertResult();
    }

    @Test
    void testQueryWithNestedFieldedPhrasesAndTerms() throws Exception {
        givenQuery("FOO:(abc (def ghi))");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND ( FOO:def AND FOO:ghi ) )");

        assertResult();
    }

    @Test
    void testQueryWithNestedFieldedTerms() throws Exception {
        givenQuery("FOO:((abc def ghi))");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( ( FOO:abc AND FOO:def AND FOO:ghi ) )");

        assertResult();
    }

    @Test
    void testQueryWithMultipleNestedFieldedPhrasesAndTerms() throws Exception {
        givenQuery("FOO:(abc (def ghi)) OR BAR:(aaa (bbb (ccc)))");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND ( FOO:def AND FOO:ghi ) )");
        expectMessage("Operator precedence may be missing, field(s): [BAR] with value(s): [aaa, bbb, ccc] will be interpreted as: ( BAR:aaa AND ( BAR:bbb AND ( BAR:ccc ) ) )");

        assertResult();
    }

    @Test
    void testQueryWithGroupedPhraseAndAmbiguousPhrase() throws Exception {
        givenQuery("FOO:abc def ghi FOO:(jkl mno)");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [jkl, mno] will be interpreted as: ( FOO:jkl AND FOO:mno )");

        assertResult();
    }

    @Test
    void testGroupedFieldedTerms() throws Exception {
        givenQuery("(FOO:abc AND BAR:def)");

        // Do not expect any results.
        assertResult();
    }

    @Test
    void testMultipleFieldsWithGroupedAmbiguousPhrases() throws Exception {
        givenQuery("(FOO:abc def ghi) OR BAR:(aaa bbb ccc) AND 333 HAT:\"111\" 222 AND HEN:car VEE:elephant zebra VEE:deer FOO:(aaa bbb)");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND def AND ghi )");
        expectMessage("Operator precedence may be missing, field(s): [BAR] with value(s): [aaa, bbb, ccc] will be interpreted as: ( BAR:aaa AND BAR:bbb AND BAR:ccc )");
        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [aaa, bbb] will be interpreted as: ( FOO:aaa AND FOO:bbb )");

        assertResult();
    }

    @Test
    void testDifferentFieldGroupedWithinTermGroup() throws Exception {
        givenQuery("FOO:(abc HAT:(def ghi)) OR BAR:(aaa (bbb (ccc)))");

        expectMessage("Operator precedence may be missing, field(s): [FOO, HAT] with value(s): [abc, def, ghi] will be interpreted as: ( FOO:abc AND ( HAT:def AND HAT:ghi ) )");
        expectMessage("Operator precedence may be missing, field(s): [BAR] with value(s): [aaa, bbb, ccc] will be interpreted as: ( BAR:aaa AND ( BAR:bbb AND ( BAR:ccc ) ) )");

        assertResult();
    }

    @Test
    void testDifferentFieldWithinTermGroup() throws Exception {
        givenQuery("FOO:(abc HAT:def ghi) OR BAR:(aaa (bbb (ccc)))");

        expectMessage("Operator precedence may be missing, field(s): [BAR] with value(s): [aaa, bbb, ccc] will be interpreted as: ( BAR:aaa AND ( BAR:bbb AND ( BAR:ccc ) ) )");

        assertResult();
    }

    @Test
    void testGroupWithUnfieldedTermsAndNonFieldValueClause() throws Exception {
        givenQuery("(FOO:abc def #ISNOTNULL(HAT))");

        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [abc, def] will be interpreted as: ( FOO:abc AND def AND #ISNOTNULL(HAT) )");

        assertResult();
    }

    @Test
    void testGroupWithGroupedAndFieldedTermsAndORGroup() throws Exception {
        givenQuery("(FOO:abc def AND BAR:ghi) OR (FOO: aaa bbb)");

        expectMessage("Operator precedence may be missing, field(s): [BAR, FOO] with value(s): [abc, def, ghi] will be interpreted as: ( ( FOO:abc AND def ) AND BAR:ghi )");
        expectMessage("Operator precedence may be missing, field(s): [FOO] with value(s): [aaa, bbb] will be interpreted as: ( FOO:aaa AND bbb )");

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
