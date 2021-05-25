package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TreeEqualityVisitorTest {
    
    @Test
    public void testNonEqualType() throws ParseException {
        assertNotEquivalent("FOO == 'bar'", "BAT == 'one' || BAR == 'two'",
                        "Did not find a matching child for EQNode in [OrNode]: Classes differ: ASTEQNode vs ASTOrNode");
    }
    
    @Test
    public void testNonEqualValue() throws ParseException {
        ASTJexlScript first = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        first.jjtSetValue("somevalue");
        ASTJexlScript second = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        assertNotEquivalent(first, second, "Node values differ: somevalue vs null");
    }
    
    @Test
    public void testNonEqualImage() throws ParseException {
        assertNotEquivalent("FOO == 'bar'", "FOO == 'bat'",
                        "Did not find a matching child for EQNode in [EQNode]: Did not find a matching child for StringLiteral in [StringLiteral]: Node images differ: bar vs bat");
    }
    
    @Test
    public void testNonEqualChildrenSize() throws ParseException {
        assertNotEquivalent(
                        "[1, 2, 3, 4]",
                        "[1, 2]",
                        "Did not find a matching child for ArrayLiteral in [ArrayLiteral]: Num children differ: [NumberLiteral, NumberLiteral, NumberLiteral, NumberLiteral] vs [NumberLiteral, NumberLiteral]");
    }
    
    @Test
    public void testNonEqualChildren() throws ParseException {
        assertNotEquivalent(
                        "[1, 2]",
                        "[1, 3]",
                        "Did not find a matching child for ArrayLiteral in [ArrayLiteral]: Did not find a matching child for NumberLiteral in [NumberLiteral]: Node images differ: 2 vs 3");
    }
    
    @Test
    public void testEqualTreesWithIdenticalNodeOrder() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "FOO == 'one' && BAT == 'two' && BAT == 'three'");
    }
    
    @Test
    public void testEqualTreesWithDifferentNodeOrder() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "BAT == 'two' && FOO == 'one' && BAT == 'three'");
    }
    
    @Test
    public void testEqualTreesWithDifferentWrapping() throws ParseException {
        assertEquivalent("FOO == 'bar'", "((FOO == 'bar'))");
    }
    
    @Test
    public void testEqualTreesWithDifferentNodeOrderAndDifferentWrapping() throws ParseException {
        assertEquivalent("FOO == 'one' && BAT == 'two' && BAT == 'three'", "((BAT == 'two' && FOO == 'one') && BAT == 'three')");
    }
    
    private void assertEquivalent(String first, String second) throws ParseException {
        ASTJexlScript firstScript = JexlASTHelper.parseJexlQuery(first);
        ASTJexlScript secondScript = JexlASTHelper.parseJexlQuery(second);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(firstScript, secondScript);
        assertTrue(comparison.isEqual());
        assertNull(comparison.getReason());
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
}
