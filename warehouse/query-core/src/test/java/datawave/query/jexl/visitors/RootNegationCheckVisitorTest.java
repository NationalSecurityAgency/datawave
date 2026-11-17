package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;

public class RootNegationCheckVisitorTest {
    @BeforeEach
    public void setup() {

    }

    @Test
    public void testNEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO != 'bar'");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testWrappedNegation() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO == 'bar')");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testWrappedNEq() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO != 'bar')");
        assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testNegatedAnd() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO == 'bar' && FOO == 'baz')");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testNegatedOneLeafAnd() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO != 'bar' && FOO == 'baz')");
        assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testNegatedMarker() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && !(FOO == 'bar' && FOO == 'baz'))");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testWrappedNegatedMarker() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!((_Delayed_ = true) && (FOO == 'bar' && FOO == 'baz'))");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testNegatedMarkerOneLeaf() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((_Delayed_ = true) && !(FOO != 'bar' && FOO == 'baz'))");
        assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testNR() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO !~ 'bar.*'");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testWrappedER() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO =~ 'bar.*')");
        assertTrue(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }

    @Test
    public void testDoubleNegatedNR() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("!(FOO !~ 'bar.*')");
        assertFalse(RootNegationCheckVisitor.hasTopLevelNegation(script));
    }
}
