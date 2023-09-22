package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;

public class TreeEqualityVisitorTest {

    /**
     * Verify that two trees where one child is not found in the other are considered non-equal.
     */
    @Test
    public void testNonEqualType() throws ParseException {
        assertNotEquivalent("FOO == 'bar'", "BAT == 'one' || BAR == 'two'",
                        "Did not find a matching child for EQNode in [OrNode]: Classes differ: ASTEQNode vs ASTOrNode");
    }

    /**
     * Verify that two nodes identical other than their values are considered equal.
     */
    @Test
    public void testNonEqualValue() throws ParseException {
        ASTJexlScript first = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        first.jjtSetValue("somevalue");
        ASTJexlScript second = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        assertEquivalent(first, second);
    }

    /**
     * Verify that two nodes identical other their images are considered non-equal.
     */
    @Test
    public void testNonEqualImage() throws ParseException {
        assertNotEquivalent("FOO == 'bar'", "FOO == 'bat'",
                        "Did not find a matching child for EQNode in [EQNode]: Did not find a matching child for bar in [bat]: Node images differ: bar vs bat");
    }

    /**
     * Verify that two nodes with differing number of children are considered non-equal.
     */
    @Test
    public void testNonEqualChildrenSize() throws ParseException {
        assertNotEquivalent("[1, 2, 3, 4]", "[1, 2]",
                        "Did not find a matching child for [ 1, 2, 3, 4 ] in [[ 1, 2 ]]: Num children differ: [1, 2, 3, 4] vs [1, 2]");
    }

    /**
     * Verify that two nodes with different children are considered non-equal.
     */
    @Test
    public void testNonEqualChildren() throws ParseException {
        assertNotEquivalent("[1, 2]", "[1, 3]",
                        "Did not find a matching child for [ 1, 2 ] in [[ 1, 3 ]]: Did not find a matching child for 2 in [3]: Node images differ: 2 vs 3");
    }

    /**
     * Verify that two identical trees are considered equal.
     */
    @Test
    public void testEqualTreesWithIdenticalNodeOrder() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "FOO == 'one' && BAT == 'two' && BAT == 'three'");
    }

    /**
     * Verify that two trees with the same children in different order are considered equal.
     */
    @Test
    public void testEqualTreesWithDifferentNodeOrder() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "BAT == 'two' && FOO == 'one' && BAT == 'three'");
    }

    /**
     * Verify that two trees with different wrapping levels are considered equal.
     */
    @Test
    public void testEqualTreesWithDifferentWrapping() throws ParseException {
        assertEquivalent("FOO == 'bar'", "((FOO == 'bar'))");
    }

    /**
     * Verify that two trees with different wrapping levels and differently ordered children are considered equal.
     */
    @Test
    public void testEqualTreesWithDifferentNodeOrderAndDifferentWrapping() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "((BAT == 'two' && FOO == 'one') && BAT == 'three')");
    }

    /**
     * Verify that a wrapped query property marker is considered equal to an unwrapped version of the same query property marker.
     */
    @Test
    public void testNonEqualTreesWithUnwrappedQueryPropertyMarker() throws ParseException {
        assertEquivalent("((_Bounded_ = true) && (NUM > '1' && NUM < '5'))", "(_Bounded_ = true) && (NUM > '1' && NUM < '5')");
    }

    /**
     * Verify that a wrapped query property marker is considered equal to the same query property marker wrapped multiple times.
     */
    @Test
    public void testEqualTreesWithExtraWrappedQueryPropertyMarker() throws ParseException {
        assertEquivalent("((_Bounded_ = true) && (NUM > '1' && NUM < '5'))", "((((_Bounded_ = true) && (NUM > '1' && NUM < '5'))))");
    }

    /**
     * Verify that query property markers that have equivalent sources in different orders are considered equal.
     */
    @Test
    public void testEqualQueryPropertyMarkersWithSourcesInDifferentOrder() throws ParseException {
        assertEquivalent("((_Bounded_ = true) && (NUM > '1' && NUM < '5'))", "((_Bounded_ = true) && (NUM < '5' && NUM > '1'))");
    }

    /**
     * Verify that a specifically typed query property marker node is considered equal to a generically parsed version of the same query property marker.
     */
    @Test
    public void testEqualTreesWithTypedQueryPropertyMarkerAndNonTypedQueryPropertyMarker() throws ParseException {
        JexlNode first = parseWithoutASTJexlScript("((_Bounded_ = true) && (NUM > '1' && NUM < '5'))");
        JexlNode second = QueryPropertyMarker.create(parseWithoutASTJexlScript("NUM > '1' && NUM < '5'"), BOUNDED_RANGE);

        assertEquivalent(first, second);
    }

    private void assertEquivalent(String first, String second) throws ParseException {
        ASTJexlScript firstScript = JexlASTHelper.parseJexlQuery(first);
        ASTJexlScript secondScript = JexlASTHelper.parseJexlQuery(second);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(firstScript, secondScript);
        assertNull(comparison.getReason());
        assertTrue(comparison.isEqual());
    }

    private void assertEquivalent(JexlNode first, JexlNode second) {
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(first, second);
        assertNull(comparison.getReason());
        assertTrue(comparison.isEqual());
    }

    private void assertNotEquivalent(String first, String second, String expectedReason) throws ParseException {
        ASTJexlScript firstScript = JexlASTHelper.parseJexlQuery(first);
        ASTJexlScript secondScript = JexlASTHelper.parseJexlQuery(second);
        assertNotEquivalent(firstScript, secondScript, expectedReason);
    }

    private void assertNotEquivalent(JexlNode first, JexlNode second, String expectedReason) {
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(first, second);
        assertFalse(comparison.isEqual());
        assertEquals(expectedReason, comparison.getReason());
    }

    private JexlNode parseWithoutASTJexlScript(String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode child = script.jjtGetChild(0);
        child.jjtSetParent(null);
        return child;
    }
}
