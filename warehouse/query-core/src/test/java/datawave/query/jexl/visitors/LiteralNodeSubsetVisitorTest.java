package datawave.query.jexl.visitors;

import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LiteralNodeSubsetVisitorTest {
    
    private final Set<String> expectedLiterals = new HashSet<>(Arrays.asList("FOO", "BAR", "BAZ", "ABCD", "1234", "1b3d"));
    
    @Test
    public void testSingleLiteralWithMatch() throws ParseException {
        String queryString = "FOO == '1234'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(1L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testSingleLiteralWithoutMatch() throws ParseException {
        String queryString = "HERPDERP == '10000000'";
        Set<String> expectedLiterals = new HashSet<>();
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(0L, literals.size());
    }
    
    @Test
    public void testMultipleLiteralsAllMatch() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'ABCD' && BAZ == '1b3d'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(3L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "ABCD"));
        Assert.assertTrue(literals.containsEntry("BAZ", "1b3d"));
    }
    
    @Test
    public void testMultipleLiteralsMixedMatch() throws ParseException {
        String queryString = "FOO == '1234' || FLARP == '9001' && BAZ == '1b3d'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(2L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAZ", "1b3d"));
    }
    
    @Test
    public void testMultipleLiteralsNoMatch() throws ParseException {
        String queryString = "YO == 'HO' || YO == 'HO' && A == 'PIRATESLIFEOFRME'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(0L, literals.size());
    }
    
    @Test
    public void testSingleLiteralWithMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(1L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testSingleLiteralWithNoMatchAndRegex() throws ParseException {
        String queryString = "LULZ == '1000000' || BAR =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(0L, literals.size());
    }
    
    @Test
    public void testMultipleLiteralsAllMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(2L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "abcd"));
    }
    
    @Test
    public void testMultipleLiteralsMixedMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && FLIM == 'FLAM' || BAZ =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(1L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testMultipleLiteralsNoMatchAndRegex() throws ParseException {
        String queryString = "FLIM == 'FLAM' && PARA == 'diddle' || CHEESE =~ 'ka.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(0L, literals.size());
    }
    
}
