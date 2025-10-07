package datawave.query.jexl.nodes;

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link LexicographicalNodeComparator} to verify expected sorts with different fields and values
 */
public class LexicographicalNodeComparatorTest extends NodeComparatorTestUtil {

    private final JexlNodeComparator comparator = new LexicographicalNodeComparator();

    // same node, same field, same value
    @Test
    public void testSameNodeType_sameField_sameValue() {
        // assert no changes
        //  @formatter:off
        String[] queries = new String[] {
                        //  intersections
                        "F == '1' && F == '1'", // eq
                        "F != '1' && F != '1'", // ne
                        "F < '1' && F < '1'", // lt
                        "F > '1' && F > '1'", // gt
                        "F <= '1' && F <= '1'", // le
                        "F >= '1' && F >= '1'", // ge
                        "F =~ '1' && F =~ '1'", // er
                        "F !~ '1' && F !~ '1'", // nr
                        "!(F == '1') && !(F == '1')", // not
                        //  unions
                        "F == '1' || F == '1'", // eq
                        "F != '1' || F != '1'", // ne
                        "F < '1' || F < '1'", // lt
                        "F > '1' || F > '1'", // gt
                        "F <= '1' || F <= '1'", // le
                        "F >= '1' || F >= '1'", // ge
                        "F =~ '1' || F =~ '1'", // er
                        "F !~ '1' || F !~ '1'", // nr
                        "!(F == '1') || !(F == '1')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }
    }

    // same node, same field, different values
    @Test
    public void testSameNodeType_sameField_differentValue() {
        // different values, correct order, no change
        //  @formatter:off
        String[] queries = new String[] {
                        //  intersections
                        "F == '1' && F == '2'", // eq
                        "F != '1' && F != '2'", // ne
                        "F < '1' && F < '2'", // lt
                        "F > '1' && F > '2'", // gt
                        "F <= '1' && F <= '2'", // le
                        "F >= '1' && F >= '2'", // ge
                        "F =~ '1' && F =~ '2'", // er
                        "F !~ '1' && F !~ '2'", // nr
                        "!(F == '1') && !(F == '2')", // not
                        //  unions
                        "F == '1' || F == '2'", // eq
                        "F != '1' || F != '2'", // ne
                        "F < '1' || F < '2'", // lt
                        "F > '1' || F > '2'", // gt
                        "F <= '1' || F <= '2'", // le
                        "F >= '1' || F >= '2'", // ge
                        "F =~ '1' || F =~ '2'", // er
                        "F !~ '1' || F !~ '2'", // nr
                        "!(F == '1') || !(F == '2')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }

        // different values, incorrect order, expect changes

        //  @formatter:off
        String[][] sortable = new String[][] {
                        //  intersections
                        {"F == '2' && F == '1'", "F == '1' && F == '2'"}, // eq
                        {"F != '2' && F != '1'", "F != '1' && F != '2'"}, // ne
                        {"F < '2' && F < '1'", "F < '1' && F < '2'"}, // lt
                        {"F > '2' && F > '1'", "F > '1' && F > '2'"}, // gt
                        {"F <= '2' && F <= '1'", "F <= '1' && F <= '2'"}, // le
                        {"F >= '2' && F >= '1'", "F >= '1' && F >= '2'"}, // ge
                        {"F =~ '2' && F =~ '1'", "F =~ '1' && F =~ '2'"}, // er
                        {"F !~ '2' && F !~ '1'", "F !~ '1' && F !~ '2'"}, // nr
                        {"!(F == '2') && !(F == '1')", "!(F == '1') && !(F == '2')"}, // not
                        //  unions
                        {"F == '2' || F == '1'", "F == '1' || F == '2'"}, // eq
                        {"F != '2' || F != '1'", "F != '1' || F != '2'"}, // ne
                        {"F < '2' || F < '1'", "F < '1' || F < '2'"}, // lt
                        {"F > '2' || F > '1'", "F > '1' || F > '2'"}, // gt
                        {"F <= '2' || F <= '1'", "F <= '1' || F <= '2'"}, // le
                        {"F >= '2' || F >= '1'", "F >= '1' || F >= '2'"}, // ge
                        {"F =~ '2' || F =~ '1'", "F =~ '1' || F =~ '2'"}, // er
                        {"F !~ '2' || F !~ '1'", "F !~ '1' || F !~ '2'"}, // nr
                        {"!(F == '2') || !(F == '1')", "!(F == '1') || !(F == '2')"}, // not
        };
        //  @formatter:on

        for (String[] query : sortable) {
            drive(query[0], query[1], comparator);
        }
    }

    // same node, different field, same values
    @Test
    public void testSameNodeType_differentField_sameValue() {
        // different fields, correct order, no change
        //  @formatter:off
        String[] queries = new String[] {
                        //  intersections
                        "F1 == '1' && F2 == '1'", // eq
                        "F1 != '1' && F2 != '1'", // ne
                        "F1 < '1' && F2 < '1'", // lt
                        "F1 > '1' && F2 > '1'", // gt
                        "F1 <= '1' && F2 <= '1'", // le
                        "F1 >= '1' && F2 >= '1'", // ge
                        "F1 =~ '1' && F2 =~ '1'", // er
                        "F1 !~ '1' && F2 !~ '1'", // nr
                        "!(F1 == '1') && !(F2 == '1')", // not
                        //  unions
                        "F1 == '1' || F2 == '1'", // eq
                        "F1 != '1' || F2 != '1'", // ne
                        "F1 < '1' || F2 < '1'", // lt
                        "F1 > '1' || F2 > '1'", // gt
                        "F1 <= '1' || F2 <= '1'", // le
                        "F1 >= '1' || F2 >= '1'", // ge
                        "F1 =~ '1' || F2 =~ '1'", // er
                        "F1 !~ '1' || F2 !~ '1'", // nr
                        "!(F1 == '1') || !(F2 == '1')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }

        // different fields, incorrect order, expect changes

        //  @formatter:off
        String[][] sortable = new String[][] {
                        //  intersections
                        {"F2 == '1' && F1 == '1'", "F1 == '1' && F2 == '1'"}, // eq
                        {"F2 != '1' && F1 != '1'", "F1 != '1' && F2 != '1'"}, // ne
                        {"F2 < '1' && F1 < '1'", "F1 < '1' && F2 < '1'"}, // lt
                        {"F2 > '1' && F1 > '1'", "F1 > '1' && F2 > '1'"}, // gt
                        {"F2 <= '1' && F1 <= '1'", "F1 <= '1' && F2 <= '1'"}, // le
                        {"F2 >= '1' && F1 >= '1'", "F1 >= '1' && F2 >= '1'"}, // ge
                        {"F2 =~ '1' && F1 =~ '1'", "F1 =~ '1' && F2 =~ '1'"}, // er
                        {"F2 !~ '1' && F1 !~ '1'", "F1 !~ '1' && F2 !~ '1'"}, // nr
                        {"!(F2 == '1') && !(F1 == '1')", "!(F1 == '1') && !(F2 == '1')"}, // not
                        //  unions
                        {"F2 == '1' || F1 == '1'", "F1 == '1' || F2 == '1'"}, // eq
                        {"F2 != '1' || F1 != '1'", "F1 != '1' || F2 != '1'"}, // ne
                        {"F2 < '1' || F1 < '1'", "F1 < '1' || F2 < '1'"}, // lt
                        {"F2 > '1' || F1 > '1'", "F1 > '1' || F2 > '1'"}, // gt
                        {"F2 <= '1' || F1 <= '1'", "F1 <= '1' || F2 <= '1'"}, // le
                        {"F2 >= '1' || F1 >= '1'", "F1 >= '1' || F2 >= '1'"}, // ge
                        {"F2 =~ '1' || F1 =~ '1'", "F1 =~ '1' || F2 =~ '1'"}, // er
                        {"F2 !~ '1' || F1 !~ '1'", "F1 !~ '1' || F2 !~ '1'"}, // nr
                        {"!(F2 == '1') || !(F1 == '1')", "!(F1 == '1') || !(F2 == '1')"}, // not
        };
        //  @formatter:on

        for (String[] query : sortable) {
            drive(query[0], query[1], comparator);
        }
    }

    // same node, different field, different values
    @Test
    public void testSameNodeType_differentField_differentValue() {
        // different fields and values, correct order, no change
        //  @formatter:off
        String[] queries = new String[] {
                        //  intersections
                        "F1 == '1' && F2 == '1' && F2 == '2'", // eq
                        "F1 != '1' && F2 != '1' && F2 != '2'", // ne
                        "F1 < '1' && F2 < '1' && F2 < '2'", // lt
                        "F1 > '1' && F2 > '1' && F2 > '2'", // gt
                        "F1 <= '1' && F2 <= '1' && F2 <= '2'", // le
                        "F1 >= '1' && F2 >= '1' && F2 >= '2'", // ge
                        "F1 =~ '1' && F2 =~ '1' && F2 =~ '2'", // er
                        "F1 !~ '1' && F2 !~ '1' && F2 !~ '2'", // nr
                        "!(F1 == '1') && !(F2 == '1') && !(F2 == '2')", // not
                        //  unions
                        "F1 == '1' || F2 == '1' || F2 == '2'", // eq
                        "F1 != '1' || F2 != '1' || F2 != '2'", // ne
                        "F1 < '1' || F2 < '1' || F2 < '2'", // lt
                        "F1 > '1' || F2 > '1' || F2 > '2'", // gt
                        "F1 <= '1' || F2 <= '1' || F2 <= '2'", // le
                        "F1 >= '1' || F2 >= '1' || F2 >= '2'", // ge
                        "F1 =~ '1' || F2 =~ '1' || F2 =~ '2'", // er
                        "F1 !~ '1' || F2 !~ '1' || F2 !~ '2'", // nr
                        "!(F1 == '1') || !(F2 == '1') || !(F2 == '2')", // not
        };
        //  @formatter:on

        for (String query : queries) {
            drive(query, query, comparator);
        }

        // different fields and values, incorrect order, change expected
        //  @formatter:off
        String[][] sortable = new String[][] {
                        //  intersections
                        {"F2 == '2' && F2 == '1' && F1 == '1'", "F1 == '1' && F2 == '1' && F2 == '2'"}, // eq
                        {"F2 != '2' && F2 != '1' && F1 != '1'", "F1 != '1' && F2 != '1' && F2 != '2'"}, // ne
                        {"F2 < '2' && F2 < '1' && F1 < '1'", "F1 < '1' && F2 < '1' && F2 < '2'"}, // lt
                        {"F2 > '2' && F2 > '1' && F1 > '1'", "F1 > '1' && F2 > '1' && F2 > '2'"}, // gt
                        {"F2 <= '2' && F2 <= '1' && F1 <= '1'", "F1 <= '1' && F2 <= '1' && F2 <= '2'"}, // le
                        {"F2 >= '2' && F2 >= '1' && F1 >= '1'", "F1 >= '1' && F2 >= '1' && F2 >= '2'"}, // ge
                        {"F2 =~ '2' && F2 =~ '1' && F1 =~ '1'", "F1 =~ '1' && F2 =~ '1' && F2 =~ '2'"}, // er
                        {"F2 !~ '2' && F2 !~ '1' && F1 !~ '1'", "F1 !~ '1' && F2 !~ '1' && F2 !~ '2'"}, // nr
                        {"!(F2 == '2') && !(F2 == '1') && !(F1 == '1')", "!(F1 == '1') && !(F2 == '1') && !(F2 == '2')"}, // not
                        //  unions
                        {"F2 == '2' || F2 == '1' || F1 == '1'", "F1 == '1' || F2 == '1' || F2 == '2'"}, // eq
                        {"F2 != '2' || F2 != '1' || F1 != '1'", "F1 != '1' || F2 != '1' || F2 != '2'"}, // ne
                        {"F2 < '2' || F2 < '1' || F1 < '1'", "F1 < '1' || F2 < '1' || F2 < '2'"}, // lt
                        {"F2 > '2' || F2 > '1' || F1 > '1'", "F1 > '1' || F2 > '1' || F2 > '2'"}, // gt
                        {"F2 <= '2' || F2 <= '1' || F1 <= '1'", "F1 <= '1' || F2 <= '1' || F2 <= '2'"}, // le
                        {"F2 >= '2' || F2 >= '1' || F1 >= '1'", "F1 >= '1' || F2 >= '1' || F2 >= '2'"}, // ge
                        {"F2 =~ '2' || F2 =~ '1' || F1 =~ '1'", "F1 =~ '1' || F2 =~ '1' || F2 =~ '2'"}, // er
                        {"F2 !~ '2' || F2 !~ '1' || F1 !~ '1'", "F1 !~ '1' || F2 !~ '1' || F2 !~ '2'"}, // nr
                        {"!(F2 == '2') || !(F2 == '1') || !(F1 == '1')", "!(F1 == '1') || !(F2 == '1') || !(F2 == '2')"}, // not
        };
        //  @formatter:on

        for (String[] query : sortable) {
            drive(query[0], query[1], comparator);
        }
    }

    @Test
    public void testDemonstrateJunctionSortOrder() {
        // this test case demonstrates why this visitor should only be used to break ties between two otherwise equivalent nodes
        String query = "F == '1' && (F == '2' || F == '3')";
        String expected = "(F == '2' || F == '3') && F == '1'";
        drive(query, expected, comparator);
    }
}
