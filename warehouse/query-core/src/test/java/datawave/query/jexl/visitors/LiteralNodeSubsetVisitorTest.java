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
        Multimap<String,String> literals = getLiterals(queryString);
        
        Assert.assertEquals(1, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testSingleLiteralWithoutMatch() throws ParseException {
        String queryString = "HERPDERP == '10000000'";
        Set<String> expectedLiterals = new HashSet<>();
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(0, literals.size());
    }
    
    @Test
    public void testMultipleLiteralsAllMatch() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'ABCD' && BAZ == '1b3d'";
        Multimap<String,String> literals = getLiterals(queryString);
        
        Assert.assertEquals(3, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "ABCD"));
        Assert.assertTrue(literals.containsEntry("BAZ", "1b3d"));
    }
    
    @Test
    public void testMultipleLiteralsMixedMatch() throws ParseException {
        String queryString = "FOO == '1234' || FLARP == '9001' && BAZ == '1b3d'";
        Multimap<String,String> literals = getLiterals(queryString);
        
        Assert.assertEquals(2, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAZ", "1b3d"));
    }
    
    @Test
    public void testMultipleLiteralsNoMatch() throws ParseException {
        String queryString = "YO == 'HO' || YO == 'HO' && A == 'PIRATESLIFEFORME'";
        
        Assert.assertEquals(0, getLiterals(queryString).size());
    }
    
    @Test
    public void testSingleLiteralWithMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        Multimap<String,String> literals = getLiterals(queryString);
        
        Assert.assertEquals(1, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testSingleLiteralWithNoMatchAndRegex() throws ParseException {
        String queryString = "LULZ == '1000000' || BAR =~ 'abcd.*\\d'";
        
        Assert.assertEquals(0, getLiterals(queryString).size());
    }
    
    @Test
    public void testMultipleLiteralsAllMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        Multimap<String,String> literals = getLiterals(queryString);
        
        Assert.assertEquals(2, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "abcd"));
    }
    
    @Test
    public void testMultipleLiteralsMixedMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && FLIM == 'FLAM' || BAZ =~ 'abcd.*\\d'";
        
        Assert.assertEquals(1, getLiterals(queryString).size());
        Assert.assertTrue(getLiterals(queryString).containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testMultipleLiteralsNoMatchAndRegex() throws ParseException {
        String queryString = "FLIM == 'FLAM' && PARA == 'diddle' || CHEESE =~ 'ka.*\\d'";
        
        Assert.assertEquals(0, getLiterals(queryString).size());
    }
    
    private Multimap<String,String> getLiterals(String queryString) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        
        return LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
    }
    
}
