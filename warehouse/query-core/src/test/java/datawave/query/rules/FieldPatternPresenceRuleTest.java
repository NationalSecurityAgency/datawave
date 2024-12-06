package datawave.query.rules;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.util.MockMetadataHelper;

public class FieldPatternPresenceRuleTest extends ShardQueryRuleTest {

    private static final MockMetadataHelper defaultMetadataHelper = new MockMetadataHelper();

    // @formatter:off
    private static final Map<String, String> fieldMessages = Map.of(
                    "FOO", "Field FOO is restricted.",
                    "TOMFOOLERY", "No tomfoolery allowed.",
                    "_ANYFIELD_", "Unfielded term _ANYFIELD_ present."
    );
    // @formatter:on

    // @formatter:off
    private static final Map<String, String> patternMessages = Map.of(
                    ".*", "Pattern too expansive.",
                    "(^_^)", "Pattern looks like a face."
    );
    // @formatter:on

    @BeforeEach
    public void setUp() throws Exception {
        givenRuleName(RULE_NAME);
        givenMetadataHelper(defaultMetadataHelper);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query with no matching fields or patterns.
     */
    @Test
    public void testNoMatchesFound() throws Exception {
        givenQuery("BAR == 'abc' || BAR =~'abc'");

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test a query where matching fields and patterns are found.
     */
    @Test
    public void testMatchesFound() throws Exception {
        givenQuery("TOMFOOLERY == 'abc' && _ANYFIELD_ =~ '.*' && FOO =~ '(^_^)' && HAT == 'def'");
        expectMessage("Field FOO is restricted.");
        expectMessage("No tomfoolery allowed.");
        expectMessage("Unfielded term _ANYFIELD_ present.");
        expectMessage("Pattern too expansive.");
        expectMessage("Pattern looks like a face.");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        FieldPatternPresenceRule rule = new FieldPatternPresenceRule(ruleName);
        rule.setFieldMessages(fieldMessages);
        rule.setPatternMessages(patternMessages);
        return rule;
    }
}
