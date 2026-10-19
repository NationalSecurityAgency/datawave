package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import datawave.query.jexl.JexlASTHelper;

public class PushdownUnindexedFieldsVisitorTest extends EasyMockSupport {

    public static Stream<Arguments> testCases() {
        // @formatter:off
        return Arrays.stream(new Object[][] {
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
        }).map(Arguments::of);
        // @formatter:on
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testCases")
    public void testPushdown(String testName, String baseQueryPlan, String expectedUnindexedPushdown, Set<String> unindexedFields) throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQueryPlan);

        replayAll();

        JexlNode result = PushdownUnindexedFieldsVisitor.pushdownPredicates(query, new HashSet<>(unindexedFields));
        assertEquals(expectedUnindexedPushdown, JexlStringBuildingVisitor.buildQuery(result));

        verifyAll();
    }

}
