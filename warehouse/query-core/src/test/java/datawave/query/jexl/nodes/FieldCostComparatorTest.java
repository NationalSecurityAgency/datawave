package datawave.query.jexl.nodes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.order.OrderByCostVisitor;
import datawave.query.util.count.CountMap;

/**
 * Tests for the {@link FieldCostComparator}
 */
public class FieldCostComparatorTest extends NodeComparatorTestUtil {

    private CountMap counts;
    private JexlNodeComparator comparator;

    // sort when all fields present in map
    @Test
    public void testAllFieldsHaveCardinality() {
        String query = "F23 == '23' || F12 == '12'";
        String expected = "F12 == '12' || F23 == '23'";

        drive(query, expected, getComparator());
    }

    // sort when some fields present in map
    @Test
    public void testSomeFieldsHaveCardinality() {
        // F11 is not found in the count map, should get sorted to the right
        String query = "F11 == '11' || F12 == '12'";
        String expected = "F12 == '12' || F11 == '11'";

        drive(query, expected, getComparator());
    }

    // sort when no fields are present in map (default ordering)
    @Test
    public void testNoFieldsHaveCardinality() {
        String query = "F2 == '2' || F1 == '1' || F2 == '1'";
        String expected = "F1 == '1' || F2 == '1' || F2 == '2'";

        drive(query, expected, getComparator());
    }

    // sort with leaves and unions

    @Test
    public void testJunctionSortsLeftOfHighCostLeaf() {
        String query = "F45 == '45' && (F12 == '12' || F23 == '23')";
        String expected = "(F12 == '12' || F23 == '23') && F45 == '45'";
        drive(query, expected, getComparator());
    }

    @Test
    public void testIntersectionSortsRightWithUniformCosts() {
        // because intersections take the lowest cost, if a leaf joins with a junction
        // and the leaf shares the lowest cost node in the junction, you get a tie
        String query = "(F12 == '12' && F23 == '23') || F12 == '12'";
        String expected = "F12 == '12' || (F12 == '12' && F23 == '23')";
        drive(query, expected, getComparator());
    }

    // sort with leaves or junctions

    // sort with unions of variable sizes
    @Test
    public void testSortUnionsOfVariableSizeAndCost() {
        // lower cardinality unions should sort first even if it has more terms
        String query = "(F45 == '45' || F45 == '45') && (F12 == '12' || F12 == '12' || F12 == '12')";
        String expected = "(F12 == '12' || F12 == '12' || F12 == '12') && (F45 == '45' || F45 == '45')";
        drive(query, expected, getComparator());
    }

    // sort with intersections of variable sizes
    @Test
    public void testSortIntersectionsOfVariableSizeAndCost() {
        // lower cardinality intersections should sort first even if it has more terms
        String query = "(F45 == '45' && F45 == '45') || (F12 == '12' && F12 == '12' && F12 == '12')";
        String expected = "(F12 == '12' && F12 == '12' && F12 == '12') || (F45 == '45' && F45 == '45')";
        drive(query, expected, getComparator());
    }

    // test integer overflow with multiple negation nodes
    @Test
    public void testNestedUnionOfNegatedTermsSortsLast() {
        String query = "(!(F == '1') || !(F == '1')) && F12 == '12'";
        String expected = "F12 == '12' && (!(F == '1') || !(F == '1'))";
        drive(query, expected, getComparator());

        query = "(F != '1' || F != '1') && F12 == '12'";
        expected = "F12 == '12' && (F != '1' || F != '1')";
        drive(query, expected, getComparator());
    }

    // test integer overflow with multiple marker nodes
    @Test
    public void testAvoidIntegerOverFlowWithMultipleMarkerNodes() {
        String query = "((_Value_ = true) && (F =~ 'aa.*')) && ((_Value_ = true) && (F =~ 'bb.*')) && F == '2'";
        String expected = "F == '2' && ((_Value_ = true) && (F =~ 'aa.*')) && ((_Value_ = true) && (F =~ 'bb.*'))";
        drive(query, expected, getComparator());
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
            script = OrderByCostVisitor.orderByFieldCount(script, getCounts().getCounts());
            String ordered = JexlStringBuildingVisitor.buildQuery(script);
            assertEquals(expected, ordered);
        } catch (Exception e) {
            fail("Failed to run test", e);
        }
    }

    private JexlNodeComparator getComparator() {
        if (comparator == null) {
            comparator = new FieldCostComparator(getCounts());
        }
        return comparator;
    }

    private CountMap getCounts() {
        if (counts == null) {
            counts = new CountMap();
            counts.put("F12", 12L);
            counts.put("F23", 23L);
            counts.put("F34", 34L);
            counts.put("F45", 45L);
        }
        return counts;
    }
}
