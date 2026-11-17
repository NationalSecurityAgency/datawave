package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;

public class PatternNodeVisitorTest {

    @Test
    public void testSingleER() throws ParseException {
        String queryString = "FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        assertEquals(1L, patterns.size());
        assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testSingleERWithOtherAndTerms() throws ParseException {
        String queryString = "BAR == '1' && FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        assertEquals(1L, patterns.size());
        assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testSingleERWithOtherOrTerms() throws ParseException {
        String queryString = "BAR == '1' || FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        assertEquals(1L, patterns.size());
        assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testMultiER() throws ParseException {
        String queryString = "BAR =~ '*over9000*?' && FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        assertEquals(2L, patterns.size());
        assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
        assertTrue(patterns.containsEntry("BAR", "*over9000*?"));
    }

    @Test
    public void testMultiERWithOtherTerms() throws ParseException {
        String queryString = "BAR =~ '*over9000*?' && FOO =~ '1234.*\\d' && BAZ == '9001'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        assertEquals(2L, patterns.size());
        assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
        assertTrue(patterns.containsEntry("BAR", "*over9000*?"));
    }

}
