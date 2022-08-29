package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LiteralNodeSubsetVisitorTest {
    
    @Test
    public void testSingleLiteralWithMatch() throws ParseException {
        String queryString = "FOO == '1234'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        test(queryString, literals);
    }
    
    @Test
    public void testSingleLiteralWithoutMatch() throws ParseException {
        String queryString = "RANDOMFIELD == '10000000'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsAllMatch() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'abcd' && BAZ == '1b3d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        literals.put("BAZ", "1b3d");
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsMixedMatch() throws ParseException {
        String queryString = "FOO == '1234' || FLARP == '9001' && BAZ == '1b3d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        literals.put("BAZ", "1b3d");
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsNoMatch() throws ParseException {
        String queryString = "YO == 'ho' || YO == 'ho' && A == 'pirateslifeforme'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(queryString, literals);
    }
    
    @Test
    public void testSingleLiteralWithMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        test(queryString, literals);
    }
    
    @Test
    public void testSingleLiteralWithNoMatchAndRegex() throws ParseException {
        String queryString = "DIFFERENTFIELD == '1000000' || BAR =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsAllMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsMixedMatchAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && FLIM == 'flam' || BAZ =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        literals.put("FOO", "1234");
        test(queryString, literals);
    }
    
    @Test
    public void testMultipleLiteralsNoMatchAndRegex() throws ParseException {
        String queryString = "FLIM == 'flam' && PARA == 'diddle' || CHEESE =~ 'ka.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(queryString, literals);
    }
    
    private void test(String query, Multimap<String,String> expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> expectedLiterals = new HashSet<>(expected.keySet());
        expectedLiterals.addAll(expected.values());
        Multimap<String,String> actualLiterals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        Assert.assertEquals(expected.size(), actualLiterals.size());
        for (Map.Entry<String,String> entry : expected.entries()) {
            Assert.assertTrue(actualLiterals.containsEntry(entry.getKey(), entry.getValue()));
        }
    }
    
}
