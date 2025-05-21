package datawave.query.planner.scanhints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class IvaratorScanHintTest {
    IvaratorScanHint hint;

    @Before
    public void setup() {
        hint = new IvaratorScanHint();
    }

    @Test
    public void testEmptyQueryApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("");
        assertFalse(hint.apply(node));
    }

    @Test
    public void testEqualityApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("FIELD == 'apple'");
        assertFalse(hint.apply(node));
    }

    @Test
    public void testEqualityAndApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("FIELD == 'apple' AND FIELD2 == 'banana'");
        assertFalse(hint.apply(node));
    }

    @Test
    public void testEqualityOrApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("FIELD == 'apple' OR FIELD2 == 'banana'");
        assertFalse(hint.apply(node));
    }

    @Test
    public void testIvaratorValueApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("BAZ == 'woot' && !((_Value_ = true) && (FOO =~ '.*a'))");
        assertTrue(hint.apply(node));
    }

    @Test
    public void testIvaratorTermApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("BAZ == 'woot' && !((_Term_ = true) && (FOO =~ '.*a'))");
        assertTrue(hint.apply(node));
    }

    @Test
    public void testIvaratorListApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("BAZ == 'woot' && !((_List_ = true) && (FOO =~ '.*a'))");
        assertTrue(hint.apply(node));
    }

    @Test
    public void testFunctionApply() throws ParseException {
        JexlNode node = JexlASTHelper.parseJexlQuery("BAZ == 'woot' && filter:isNotNull(FOO)");
        assertFalse(hint.apply(node));
    }

    @Test
    public void testDefaults() {
        assertEquals("shard", hint.getTable());
        assertEquals("scan_type", hint.getHintName());
        assertEquals("ivarator", hint.getHintValue());
    }

    @Test
    public void testOverrides() {
        hint.setTable("t2");
        hint.setHintName("my_hint");
        hint.setHintValue("my_val");

        assertEquals("t2", hint.getTable());
        assertEquals("my_hint", hint.getHintName());
        assertEquals("my_val", hint.getHintValue());
    }
}
