package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    
    @Test
    public void testExcludeRegex1() throws ParseException {
        String query = "filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(query, literals);
    }
    
    @Test
    public void testExcludeRegex2() throws ParseException {
        String query = "FOO == 'bar' && filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");
        
        test(query, literals);
    }
    
    @Test
    public void testExcludeRegex3() throws ParseException {
        String query = "FOO == 'bar' || filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");
        
        test(query, literals);
    }
    
    @Test
    public void testIncludeRegex1() throws ParseException {
        String query = "filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        
        test(query, literals);
    }
    
    @Test
    public void testIncludeRegex2() throws ParseException {
        String query = "FOO == 'bar' && filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");
        
        test(query, literals);
    }
    
    @Test
    public void testIncludeRegex3() throws ParseException {
        String query = "FOO == 'bar' || filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");
        
        test(query, literals);
    }
    
    @Test
    public void testEqNodeInsideQueryPropertyMarker() throws ParseException {
        String query = "((_Delayed_ = true) && FOO == 'bar')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");
        
        test(query, literals);
    }
    
    private void test(String query, Multimap<String,String> expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> expectedLiterals = new HashSet<>(expected.keySet());
        expectedLiterals.addAll(expected.values());
        Multimap<String,String> actualLiterals = LiteralNodeSubsetVisitor.getLiterals(expectedLiterals, script);
        
        assertEquals(expected.size(), actualLiterals.size());
        for (Map.Entry<String,String> entry : expected.entries()) {
            assertTrue(actualLiterals.containsEntry(entry.getKey(), entry.getValue()));
        }
    }
    
}
