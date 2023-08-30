package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;

public class PatternNodeVisitorTest {

    @Test
    public void testSingleER() throws ParseException {
        String queryString = "FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        Assert.assertEquals(1L, patterns.size());
        Assert.assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testSingleERWithOtherAndTerms() throws ParseException {
        String queryString = "BAR == '1' && FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        Assert.assertEquals(1L, patterns.size());
        Assert.assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testSingleERWithOtherOrTerms() throws ParseException {
        String queryString = "BAR == '1' || FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        Assert.assertEquals(1L, patterns.size());
        Assert.assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
    }

    @Test
    public void testMultiER() throws ParseException {
        String queryString = "BAR =~ '*over9000*?' && FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        Assert.assertEquals(2L, patterns.size());
        Assert.assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
        Assert.assertTrue(patterns.containsEntry("BAR", "*over9000*?"));
    }

    @Test
    public void testMultiERWithOtherTerms() throws ParseException {
        String queryString = "BAR =~ '*over9000*?' && FOO =~ '1234.*\\d' && BAZ == '9001'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> patterns = PatternNodeVisitor.getPatterns(script);

        Assert.assertEquals(2L, patterns.size());
        Assert.assertTrue(patterns.containsEntry("FOO", "1234.*\\d"));
        Assert.assertTrue(patterns.containsEntry("BAR", "*over9000*?"));
    }

}
