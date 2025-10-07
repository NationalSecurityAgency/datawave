package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmbiguousOrPhrasesRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a basic query with no ambiguous phrases.
     */
    @Test
    void testQueryWithoutAmbiguousPhrase() throws Exception {
        givenQuery("FOO:abc");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with wrapped fielded phrases.
     */
    @Test
    void testQueryWithWrappedPhrase() throws Exception {
        givenQuery("FOO:(abc OR def)");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with quoted phrases.
     */
    @Test
    void testQueryWithQuotedPhrases() throws Exception {
        givenQuery("FOO:\"abc\" OR \"def\"");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with nested wrapped fielded phrases.
     */
    @Test
    void testQueryWithNestedWrappedPhrase() throws Exception {
        givenQuery("FOO:(((abc OR def)))");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with a single fielded phrase that is wrapped.
     */
    @Test
    void testQueryWithWrappedSingleFieldedPhrase() throws Exception {
        givenQuery("(FOO:abc)");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query where ambiguous phrases are separated by an AND.
     */
    @Test
    void testAndedPhrase() throws Exception {
        givenQuery("FOO:abc AND def");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with multiple grouped and ORed phrases.
     */
    @Test
    void testValidPhraseWithMultipleGroupedOrs() throws Exception {
        givenQuery("(FOO:(abc OR def)) OR ((BAR:efg AND HAT:(aaa OR bbb OR ccc))) AND #INCLUDE(FOO,'abc*')");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with unfielded ambiguous ORed phrases.
     */
    @Test
    void testQueryWithUnfieldedAmbiguousPhraseOnly() throws Exception {
        givenQuery("abc OR def OR efg");

        // Do not expect any results.
        assertResult();
    }

    /**
     * Test a query with ambiguous ORed phrases.
     */
    @Test
    void testQueryWithAmbiguousPhrase() throws Exception {
        givenQuery("FOO:abc OR def OR efg");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: FOO:abc OR def OR efg Recommended: FOO:(abc OR def OR efg)");

        assertResult();
    }

    /**
     * Test a query with ambiguous ORed phrases that are wrapped outside the fielded term.
     */
    @Test
    void testWrappedQueryWithAmbiguousPhrase() throws Exception {
        givenQuery("(FOO:abc OR def OR efg)");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: ( FOO:abc OR def OR efg ) Recommended: FOO:(abc OR def OR efg)");

        assertResult();
    }

    /**
     * Test a query with nested ambiguous ORed phrases that could be flattened.
     */
    @Test
    void testQueryWithNestedAmbiguousPhrases() throws Exception {
        givenQuery("(FOO:abc OR (def OR efg))");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: ( FOO:abc OR ( def OR efg ) ) Recommended: FOO:(abc OR def OR efg)");

        assertResult();
    }

    /**
     * Test a query with a variety of ambiguous ORed phrases, some of which should be flagged.
     */
    @Test
    void testQueryWithMultipleAmbiguousPhrases() throws Exception {
        givenQuery("FOO:aaa AND bbb AND (BAR:aaa OR bbb OR ccc OR HAT:\"ear\" nose) OR (aaa OR bbb OR VEE:eee OR 123 OR (gee OR \"wiz\")) AND (EGG:yolk OR shell)");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: BAR:aaa OR bbb OR ccc Recommended: BAR:(aaa OR bbb OR ccc)");
        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: VEE:eee OR 123 Recommended: VEE:(eee OR 123)");
        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: ( EGG:yolk OR shell ) Recommended: EGG:(yolk OR shell)");

        assertResult();
    }

    /**
     * Test a query with nested wrapped fielded phrases.
     */
    @Test
    void testQueryWithAmbiguousPhraseInSeparateGroups() throws Exception {
        givenQuery("((FOO:abc OR def) OR (aaa OR bbb))");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: ( FOO:abc OR def ) Recommended: FOO:(abc OR def)");

        assertResult();
    }

    /**
     * Test a query with consecutive groupings of ambiguous phrases.
     */
    @Test
    void testQueryWithConsecutiveAmbiguousPhrases() throws Exception {
        givenQuery("FOO:abc OR def OR BAR:aaa OR bbb");

        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: FOO:abc OR def Recommended: FOO:(abc OR def)");
        expectMessage("Ambiguous unfielded terms OR'd with fielded term detected: BAR:aaa OR bbb Recommended: BAR:(aaa OR bbb)");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToLucene();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new AmbiguousOrPhrasesRule(ruleName);
    }

}
