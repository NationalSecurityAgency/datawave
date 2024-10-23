package datawave.query.jexl.nodes;

import org.junit.Test;

public class DefaultJexlNodeComparatorTest extends NodeComparatorTestUtil {

    private final JexlNodeComparator comparator = new DefaultJexlNodeComparator();

    @Test
    public void testSortSameFieldDifferentValues() {
        String query = "FOO == 'baz' || FOO == 'bar'";
        String expected = "FOO == 'bar' || FOO == 'baz'";
        drive(query, expected, comparator);
    }

    @Test
    public void testDifferentFieldSameValues() {
        String query = "FOO_B == 'baz' || FOO_A == 'baz'";
        String expected = "FOO_A == 'baz' || FOO_B == 'baz'";
        drive(query, expected, comparator);
    }

    @Test
    public void testSortOrderWithNodePairs() {
        // EQ before NE
        String query = "FOO != 'bar' || FOO == 'bar'";
        String expected = "FOO == 'bar' || FOO != 'bar'";
        drive(query, expected, comparator);
    }

    @Test
    public void testSortSingleNodesBeforeJunctions() {
        String query = "(FOO == 'bar' && FOO == 'baz') || FOO == 'fizz'";
        String expected = "FOO == 'fizz' || (FOO == 'bar' && FOO == 'baz')";
        drive(query, expected, comparator);

        query = "(FOO == 'bar' || FOO == 'baz') && FOO == 'fizz'";
        expected = "FOO == 'fizz' && (FOO == 'bar' || FOO == 'baz')";
        drive(query, expected, comparator);
    }

    @Test
    public void testMarkersSortLast() {
        String query = "B == '2' && ((_Value_ = true) && (A =~ 'ba.*')) && A == '1'";
        String expected = "A == '1' && B == '2' && ((_Value_ = true) && (A =~ 'ba.*'))";
        drive(query, expected, comparator);
    }

}
