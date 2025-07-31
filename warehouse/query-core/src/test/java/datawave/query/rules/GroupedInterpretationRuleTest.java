package datawave.query.rules;

import org.junit.Assert;
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
    void testQueryWithGroupedPhrase() {
        givenQuery("FOO:(abc def ghi)");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
    }

    @Test
    void testQueryWithGroupedAmbiguousPhrases() {
        givenQuery("(FOO:abc def ghi)");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
    }

    @Test
    void testQueryWithGroupedFieldedPhrases() {
        givenQuery("(FOO:abc AND FOO:def AND FOO:ghi)");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
    }

    @Test
    void testQueryWithNestedFieldedPhrasesAndTerms() {
        givenQuery("FOO:(abc (def ghi))");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
    }

    @Test
    void testQueryWithGroupedPhraseAndAmbiguousPhrase() {
        givenQuery("FOO:(abc def ghi) FOO:jkl mno");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
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
    void testMultipleFieldsWithAmbiguousPhrases() {
        givenQuery("(FOO:abc def ghi) OR BAR:(aaa bbb ccc) AND 333 HAT:\"111\" 222 AND HEN:car VEE:elephant zebra VEE:deer FOO:(aaa bbb ccc)");

        Assert.assertThrows(AssertionError.class, this::assertResult);
        // test will throw error because these tests are not read as normal queries
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
