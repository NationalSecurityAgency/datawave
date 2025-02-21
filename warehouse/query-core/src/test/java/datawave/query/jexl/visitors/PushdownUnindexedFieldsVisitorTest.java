package datawave.query.jexl.visitors;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import datawave.query.jexl.JexlASTHelper;

@RunWith(Parameterized.class)
public class PushdownUnindexedFieldsVisitorTest extends EasyMockSupport {

    @Parameterized.Parameters(name = "{0}")
    public static Collection testCases() {
        // @formatter:off
        return Arrays.asList(new Object[][] {
            {
                "BasicExpressionsIndexed",
                "INDEXED_FIELD == 'a' || INDEXED_FIELD != 'a' || " +
                        "INDEXED_FIELD < 'a' || INDEXED_FIELD > 'a' || " +
                        "INDEXED_FIELD <= 'a' || INDEXED_FIELD >= 'a' || " +
                        "INDEXED_FIELD =~ 'a' || INDEXED_FIELD !~ 'a' || " +
                        "INDEXED_FIELD =^ 'a' || INDEXED_FIELD !^ 'a' || " +
                        "INDEXED_FIELD =$ 'a' || INDEXED_FIELD !$ 'a'",
                "INDEXED_FIELD == 'a' || INDEXED_FIELD != 'a' || " +
                        "INDEXED_FIELD < 'a' || INDEXED_FIELD > 'a' || " +
                        "INDEXED_FIELD <= 'a' || INDEXED_FIELD >= 'a' || " +
                        "INDEXED_FIELD =~ 'a' || INDEXED_FIELD !~ 'a' || " +
                        "INDEXED_FIELD =^ 'a' || INDEXED_FIELD !^ 'a' || " +
                        "INDEXED_FIELD =$ 'a' || INDEXED_FIELD !$ 'a'",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "BasicExpressionsUnindexed",
                "UNINDEXED_FIELD == 'a' || UNINDEXED_FIELD != 'a' || " +
                        "UNINDEXED_FIELD < 'a' || UNINDEXED_FIELD > 'a' || " +
                        "UNINDEXED_FIELD <= 'a' || UNINDEXED_FIELD >= 'a' || " +
                        "UNINDEXED_FIELD =~ 'a' || UNINDEXED_FIELD !~ 'a' || " +
                        "UNINDEXED_FIELD =^ 'a' || UNINDEXED_FIELD !^ 'a' || " +
                        "UNINDEXED_FIELD =$ 'a' || UNINDEXED_FIELD !$ 'a'",
                "((_Eval_ = true) && (UNINDEXED_FIELD == 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD != 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD < 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD > 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD <= 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD >= 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD =~ 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD !~ 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD =^ 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD !^ 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD =$ 'a')) || " +
                        "((_Eval_ = true) && (UNINDEXED_FIELD !$ 'a'))",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "NestedNegatedEquality",
                "INDEXED_FIELD == 'a' && (!(UNINDEXED_FIELD == 'b') || !(filter:includeRegex(UNINDEXED_FIELD, '.*')))",
                "INDEXED_FIELD == 'a' && (!((_Eval_ = true) && (UNINDEXED_FIELD == 'b')) || !(filter:includeRegex(UNINDEXED_FIELD, '.*')))",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "NestedInequality",
                "INDEXED_FIELD == 'a' && ((UNINDEXED_FIELD != 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*'))) && EVENT_FIELD == 'd'",
                "INDEXED_FIELD == 'a' && (((_Eval_ = true) && (UNINDEXED_FIELD != 'b')) || !(filter:includeRegex(INDEXED_FIELD, '.*'))) && EVENT_FIELD == 'd'",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "Range",
                "INDEXED_FIELD == 'a' && ((_Bounded_ = true) && (UNINDEXED_FIELD >= 'b') && (UNINDEXED_FIELD <= 'c'))",
                "INDEXED_FIELD == 'a' && ((_Eval_ = true) && ((_Bounded_ = true) && (UNINDEXED_FIELD >= 'b') && (UNINDEXED_FIELD <= 'c')))",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "RegexIvarator",
                "INDEXED_FIELD == 'a' && ((_Value_ = true) && (UNINDEXED_FIELD =~ 'b.*'))",
                "INDEXED_FIELD == 'a' && ((_Eval_ = true) && (UNINDEXED_FIELD =~ 'b.*'))",
                Collections.singleton("UNINDEXED_FIELD")
            },
            {
                "DelayedIvarator",
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (UNINDEXED_FIELD =~ 'b.*'))",
                "INDEXED_FIELD == 'a' && ((_Eval_ = true) && (UNINDEXED_FIELD =~ 'b.*'))",
                Collections.singleton("UNINDEXED_FIELD")
            }
        });
        // @formatter:on
    }

    // for parameterized set
    private final String baseQueryPlan;
    private final String expectedUnindexedPushdown;

    // internal
    private final HashSet<String> unindexedFields;

    public PushdownUnindexedFieldsVisitorTest(String testName, String baseQueryPlan, String expectedUnindexedPushdown, Set<String> unindexedFields) {
        this.baseQueryPlan = baseQueryPlan;
        this.expectedUnindexedPushdown = expectedUnindexedPushdown;
        this.unindexedFields = new HashSet<>(unindexedFields);
    }

    @Before
    public void setup() throws TableNotFoundException {}

    @Test
    public void testPushdown() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQueryPlan);

        replayAll();

        JexlNode result = PushdownUnindexedFieldsVisitor.pushdownPredicates(query, unindexedFields);
        Assert.assertEquals(expectedUnindexedPushdown, JexlStringBuildingVisitor.buildQuery(result));

        verifyAll();
    }

}
