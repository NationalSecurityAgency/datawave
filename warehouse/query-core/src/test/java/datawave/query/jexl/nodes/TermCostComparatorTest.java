package datawave.query.jexl.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.order.OrderByCostVisitor;
import datawave.query.util.count.CountMap;

public class TermCostComparatorTest extends NodeComparatorTestUtil {

    private CountMap counts;
    private JexlNodeComparator comparator;

    // sort all terms have cardinality
    @Test
    public void testAllTermsHaveCardinality() {
        String[][] queries = {{"F == '23' || F == '12'", "F == '12' || F == '23'"}, {"F == '23' && F == '12'", "F == '12' && F == '23'"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    // sort some terms have cardinality
    @Test
    public void testSomeTermsHaveCardinality() {
        String[][] queries = {{"F == '0' || F == '12'", "F == '12' || F == '0'"}, {"F == '0' && F == '12'", "F == '12' && F == '0'"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    // sort no terms have cardinality (fallback)
    @Test
    public void testNoTermsHaveCardinality() {
        String[][] queries = {{"F == '2' || F == '1'", "F == '1' || F == '2'"}, {"F == '2' && F == '1'", "F == '1' && F == '2'"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    // sort junctions all terms have cardinality + variable size
    @Test
    public void testJunctionsSortLeftOfHighCostLeaf() {
        String[][] queries = {{"(F == '12' || F == '23') && F == '45'", "(F == '12' || F == '23') && F == '45'"},
                {"(F == '12' && F == '23') || F == '45'", "(F == '12' && F == '23') || F == '45'"},
                // sort order applied to nested junctions
                {"(F == '23' || F == '12') && F == '45'", "(F == '12' || F == '23') && F == '45'"},
                {"(F == '23' && F == '12') || F == '45'", "(F == '12' && F == '23') || F == '45'"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    @Test
    public void testJunctionSort() {
        String[][] queries = {
                // assert no change ordered nested junctions and ordered top level junction
                {"(F == '12' || F == '23') && (F == '34' || F == '45')", "(F == '12' || F == '23') && (F == '34' || F == '45')"},
                {"(F == '12' && F == '23') || (F == '34' && F == '45')", "(F == '12' && F == '23') || (F == '34' && F == '45')"},
                // assert unordered nested junctions and ordered top level junctions
                {"(F == '23' || F == '12') && (F == '45' || F == '34')", "(F == '12' || F == '23') && (F == '34' || F == '45')"},
                {"(F == '23' && F == '12') || (F == '45' && F == '34')", "(F == '12' && F == '23') || (F == '34' && F == '45')"},
                // assert ordered nested junctions and unordered top level junctions
                {"(F == '34' || F == '45') && (F == '12' || F == '23')", "(F == '12' || F == '23') && (F == '34' || F == '45')"},
                {"(F == '34' && F == '45') || (F == '12' && F == '23')", "(F == '12' && F == '23') || (F == '34' && F == '45')"},
                // assert unordered nested junctions and unordered top level junctions
                {"(F == '45' || F == '34') && (F == '23' || F == '12')", "(F == '12' || F == '23') && (F == '34' || F == '45')"},
                {"(F == '45' && F == '34') || (F == '23' && F == '12')", "(F == '12' && F == '23') || (F == '34' && F == '45')"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    @Test
    public void testJunctionsOfVariableSize() {
        String[][] queries = {
                // ordered junctions
                {"(F == '12' || F == '12' || F == '12') && (F == '34' || F == '45')", "(F == '12' || F == '12' || F == '12') && (F == '34' || F == '45')"},
                {"(F == '12' && F == '12' && F == '12') || (F == '34' && F == '45')", "(F == '12' && F == '12' && F == '12') || (F == '34' && F == '45')"},
                // unordered junctions
                {"(F == '34' || F == '45') && (F == '12' || F == '12' || F == '12')", "(F == '12' || F == '12' || F == '12') && (F == '34' || F == '45')"},
                {"(F == '34' && F == '45') || (F == '12' && F == '12' && F == '12')", "(F == '12' && F == '12' && F == '12') || (F == '34' && F == '45')"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    // sort junctions partial cardinality
    @Test
    public void testJunctionsWithPartialCardinality() {
        String[][] queries = {{"F == '1' || F == '23'", "F == '23' || F == '1'"}, {"F == '1' && F == '23'", "F == '23' && F == '1'"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    // sort junctions one side has cardinality the other does not
    @Test
    public void testSomeJunctionsHaveCardinality() {
        String[][] queries = {{"(F == '1' || F == '2') && (F == '12' || F == '23')", "(F == '12' || F == '23') && (F == '1' || F == '2')"},
                {"(F == '1' && F == '2') || (F == '12' && F == '23')", "(F == '12' && F == '23') || (F == '1' && F == '2')"},};

        for (String[] query : queries) {
            drive(query[0], query[1], getComparator());
        }
    }

    /**
     * Explicit override of test utility code so the {@link OrderByCostVisitor} can be run
     *
     * @param query
     *            the input query
     * @param expected
     *            the expected query
     * @param comparator
     *            the comparator
     */
    @Override
    public void drive(String query, String expected, JexlNodeComparator comparator) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            script = OrderByCostVisitor.orderByTermCount(script, getCounts().getCounts());
            String ordered = JexlStringBuildingVisitor.buildQuery(script);
            assertEquals(expected, ordered);
        } catch (Exception e) {
            fail("Failed to run test", e);
        }
    }

    private JexlNodeComparator getComparator() {
        if (comparator == null) {
            comparator = new TermCostComparator(getCounts());
        }
        return comparator;
    }

    private CountMap getCounts() {
        if (counts == null) {
            counts = new CountMap();
            counts.put("F == '12'", 12L);
            counts.put("F == '23'", 23L);
            counts.put("F == '34'", 34L);
            counts.put("F == '45'", 45L);
        }
        return counts;
    }
}
