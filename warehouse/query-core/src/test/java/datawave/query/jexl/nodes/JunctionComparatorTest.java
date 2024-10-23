package datawave.query.jexl.nodes;

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link JunctionComparator} to verify that leaf nodes sort before junctions
 */
public class JunctionComparatorTest extends NodeComparatorTestUtil {

    private final JexlNodeComparator comparator = new JunctionComparator();

    /**
     * Test that asserts no changes to queries of the following types
     * <p>
     * <code>A &amp;&amp; B</code>
     * </p>
     */
    @Test
    public void testIntersectionOfLeafNodes() {
        //  @formatter:off
        String[] queries = new String[] {
                "F == '1' && F == '2'", // eq
                "F != '1' && F == '2'", // ne
                "F < '1' && F == '2'", // lt
                "F > '1' && F == '2'", // gt
                "F <= '1' && F == '2'", // le
                "F >= '1' && F == '2'", // ge
                "F =~ '1' && F == '2'", // er
                "F !~ '1' && F == '2'", // nr
                "!(F == '1') && F == '2'", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }
    }

    // A || B
    @Test
    public void testUnionOfLeafNodes() {
        //  @formatter:off
        String[] queries = new String[] {
                        "F == '1' || F == '2'", // eq
                        "F != '1' || F == '2'", // ne
                        "F < '1' || F == '2'", // lt
                        "F > '1' || F == '2'", // gt
                        "F <= '1' || F == '2'", // le
                        "F >= '1' || F == '2'", // ge
                        "F =~ '1' || F == '2'", // er
                        "F !~ '1' || F == '2'", // nr
                        "!(F == '1') || F == '2'", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }
    }

    // A && (B || C)
    @Test
    public void testIntersectionWithNestedUnion() {
        // first, assert queries with no change
        //  @formatter:off
        String[] queries = new String[] {
                        "F == '1' && (F == '2' || F == '3')", // eq
                        "F != '1' && (F == '2' || F == '3')", // ne
                        "F < '1' && (F == '2' || F == '3')", // lt
                        "F > '1' && (F == '2' || F == '3')", // gt
                        "F <= '1' && (F == '2' || F == '3')", // le
                        "F >= '1' && (F == '2' || F == '3')", // ge
                        "F =~ '1' && (F == '2' || F == '3')", // er
                        "F !~ '1' && (F == '2' || F == '3')", // nr
                        "!(F == '1') && (F == '2' || F == '3')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }

        // next, assert queries with change to sort order
        //  @formatter:off
        String[][] sortable = new String[][] {
                {"(F == '2' || F == '3') && F == '1'", "F == '1' && (F == '2' || F == '3')"},   //  eq
                {"(F == '2' || F == '3') && F != '1'", "F != '1' && (F == '2' || F == '3')"},   //  ne
                {"(F == '2' || F == '3') && F < '1'", "F < '1' && (F == '2' || F == '3')"}, // lt
                {"(F == '2' || F == '3') && F > '1'", "F > '1' && (F == '2' || F == '3')"}, // gt
                {"(F == '2' || F == '3') && F <= '1'", "F <= '1' && (F == '2' || F == '3')"},   // le
                {"(F == '2' || F == '3') && F >= '1'", "F >= '1' && (F == '2' || F == '3')"},   //  ge
                {"(F == '2' || F == '3') && F =~ '1'", "F =~ '1' && (F == '2' || F == '3')"},   //  er
                {"(F == '2' || F == '3') && F !~ '1'", "F !~ '1' && (F == '2' || F == '3')"},   //  nr
                {"(F == '2' || F == '3') && !(F == '1')", "!(F == '1') && (F == '2' || F == '3')"}  //  not
        };
        //  @formatter:off

        for (String[] query : sortable) {
            drive(query[0], query[1], comparator);
        }
    }

    // A || (B && C)
    @Test
    public void testUnionWithNestedIntersection() {
        // first, assert queries with no change
        //  @formatter:off
        String[] queries = new String[] {
                        "F == '1' || (F == '2' && F == '3')", // eq
                        "F != '1' || (F == '2' && F == '3')", // ne
                        "F < '1' || (F == '2' && F == '3')", // lt
                        "F > '1' || (F == '2' && F == '3')", // gt
                        "F <= '1' || (F == '2' && F == '3')", // le
                        "F >= '1' || (F == '2' && F == '3')", // ge
                        "F =~ '1' || (F == '2' && F == '3')", // er
                        "F !~ '1' || (F == '2' && F == '3')", // nr
                        "!(F == '1') || (F == '2' && F == '3')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }

        // next, assert queries with change to sort order
        //  @formatter:off
        String[][] sortable = new String[][] {
                        {"(F == '2' && F == '3') || F == '1'", "F == '1' || (F == '2' && F == '3')"},   //  eq
                        {"(F == '2' && F == '3') || F != '1'", "F != '1' || (F == '2' && F == '3')"},   //  ne
                        {"(F == '2' && F == '3') || F < '1'", "F < '1' || (F == '2' && F == '3')"}, // lt
                        {"(F == '2' && F == '3') || F > '1'", "F > '1' || (F == '2' && F == '3')"}, // gt
                        {"(F == '2' && F == '3') || F <= '1'", "F <= '1' || (F == '2' && F == '3')"},   // le
                        {"(F == '2' && F == '3') || F >= '1'", "F >= '1' || (F == '2' && F == '3')"},   //  ge
                        {"(F == '2' && F == '3') || F =~ '1'", "F =~ '1' || (F == '2' && F == '3')"},   //  er
                        {"(F == '2' && F == '3') || F !~ '1'", "F !~ '1' || (F == '2' && F == '3')"},   //  nr
                        {"(F == '2' && F == '3') || !(F == '1')", "!(F == '1') || (F == '2' && F == '3')"}  //  not
        };
        //  @formatter:off

        for (String[] query : sortable) {
            drive(query[0], query[1], comparator);
        }
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersectionOfNestedUnions() {
        //  assert no changes
        //  @formatter:off
        String[] queries = new String[] {
                        "(F == '1' || F == '2') && (F == '3' || F == '4')", // eq
                        "(F == '1' || F != '2') && (F != '3' || F == '4')", // ne
                        "(F == '1' || F < '2') && (F < '3' || F == '4')", // lt
                        "(F == '1' || F > '2') && (F > '3' || F == '4')", // gt
                        "(F == '1' || F <= '2') && (F <= '3' || F == '4')", // le
                        "(F == '1' || F >= '2') && (F >= '3' || F == '4')", // ge
                        "(F == '1' || F =~ '2') && (F =~ '3' || F == '4')", // er
                        "(F == '1' || F !~ '2') && (F !~ '3' || F == '4')", // nr
                        "(F == '1' || !(F == '2')) && (!(F == '3') || F == '4')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }
    }

    // (A && B) || (C && D)
    @Test
    public void testUnionOfNestedIntersections() {
        // assert no changes
        //  @formatter:off
        String[] queries = new String[] {
                        "(F == '1' && F == '2') || (F == '3' && F == '4')", // eq
                        "(F == '1' && F != '2') || (F != '3' && F == '4')", // ne
                        "(F == '1' && F < '2') || (F < '3' && F == '4')", // lt
                        "(F == '1' && F > '2') || (F > '3' && F == '4')", // gt
                        "(F == '1' && F <= '2') || (F <= '3' && F == '4')", // le
                        "(F == '1' && F >= '2') || (F >= '3' && F == '4')", // ge
                        "(F == '1' && F =~ '2') || (F =~ '3' && F == '4')", // er
                        "(F == '1' && F !~ '2') || (F !~ '3' && F == '4')", // nr
                        "(F == '1' && !(F == '2')) || (!(F == '3') && F == '4')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }
    }
}
