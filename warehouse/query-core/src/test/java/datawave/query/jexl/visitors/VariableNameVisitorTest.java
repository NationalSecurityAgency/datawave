package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

class VariableNameVisitorTest {

    @Test
    void testLeaves() {
        //  @formatter:off
        Object[][] queries = new Object[][] {
                        {"F == null", Collections.singleton("F")},
                        {"F == '1'", Collections.singleton("F")},
                        {"F != '1'", Collections.singleton("F")},
                        {"F > '1'", Collections.singleton("F")},
                        {"F < '1'", Collections.singleton("F")},
                        {"F >= '1'", Collections.singleton("F")},
                        {"F <= '1'", Collections.singleton("F")}
        };
        //  @formatter:on

        test(queries);
    }

    @Test
    void testJunctions() {
        //  @formatter:off
        Object[][] queries = new Object[][] {
                        // simple junctions
                        {"F1 == '1' || F2 == '2'", Sets.newHashSet("F1", "F2")},
                        {"F1 == '1' && F2 == '2'", Sets.newHashSet("F1", "F2")},
                        // single nested junction
                        {"F1 == '1' || (F2 == '2' && F3 == '3')", Sets.newHashSet("F1", "F2", "F3")},
                        {"F1 == '1' && (F2 == '2' || F3 == '3')", Sets.newHashSet("F1", "F2", "F3")},
                        // double nested junctions
                        {"(F1 == '1' && F2 == '2') || (F3 == '3' || F4 == '4')", Sets.newHashSet("F1", "F2", "F3", "F4")},
                        {"(F1 == '1' || F2 == '2') && (F3 == '3' && F4 == '4')", Sets.newHashSet("F1", "F2", "F3", "F4")}
        };
        //  @formatter:on

        test(queries);
    }

    @Test
    void testMarkers() {
        //  @formatter:off
        Object[][] queries = new Object[][] {
                        {"((_Bounded_ = true) && (F > '2' && F < '5'))",  Sets.newHashSet("F", "_Bounded_")},
                        {"((_Delayed_ = true) && (F == '1'))", Sets.newHashSet("F", "_Delayed_")},
                        {"((_Eval_ = true) && (F == '1'))", Sets.newHashSet("F", "_Eval_")},
                        {"((_List_ = true) && ((id = 'id') && (field = 'F') && (params = '{\"ranges\":[[\"[r1\",\"r2]\"],[\"[r3\",\"f4]\"]]}')))", Collections.singleton("F")},
                        {"((_Value_ = true) && (F =~ 'ba.*'))", Sets.newHashSet("F", "_Value_")},
                        {"((_Term_ = true) && (_ANYFIELD_ =~ 'ba.*'))", Sets.newHashSet("_ANYFIELD_", "_Term_")},
                        {"((_Hole_ = true) && (F == '1'))", Sets.newHashSet("F", "_Hole_")},
                        {"((_Drop_ = true) && (F == '1'))", Sets.newHashSet("F", "_Drop_")},
                        {"((_Lenient_ = true) && (F == '1'))", Sets.newHashSet("F", "_Lenient_")},
                        {"((_Strict_ = true) && (F == '1'))", Sets.newHashSet("F", "_Strict_")}
        };
        //  @formatter:on

        test(queries);
    }

    @Test
    void testQueryWithDroppedNodes() {
        // document this use case
        String query = "(((_Drop_ = true) && ((_Query_ = 'AGE > \\'abc10\\'') && (_Reason_ = 'Normalizations failed and not strict'))) || ((_Drop_ = true) && ((_Query_ = 'ETA > \\'abc10\\'') && (_Reason_ = 'Normalizations failed and not strict'))))";
        Set<String> expected = Sets.newHashSet("_Drop_", "_Query_", "_Reason_");
        test(query, expected);
    }

    @SuppressWarnings("unchecked")
    private void test(Object[][] queries) {
        for (Object[] query : queries) {
            test((String) query[0], (Set<String>) query[1]);
        }
    }

    private void test(String query, Set<String> expected) {
        try {
            Set<String> names = VariableNameVisitor.parseQuery(query);
            assertEquals(expected, names);
        } catch (ParseException e) {
            fail("Failed to parse: " + query);
            throw new IllegalArgumentException("Failed to parse query: " + query);
        }
    }
}
