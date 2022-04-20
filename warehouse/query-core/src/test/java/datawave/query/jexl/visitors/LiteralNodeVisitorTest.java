package datawave.query.jexl.visitors;

import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class LiteralNodeVisitorTest {
    
    @Test
    public void testSingleLiteral() throws ParseException {
        String queryString = "FOO == '1234'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        
        Assert.assertEquals(1L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testMultipleLiterals() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'ABCD' && BAZ == '1b3d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        
        Assert.assertEquals(3L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "ABCD"));
        Assert.assertTrue(literals.containsEntry("BAZ", "1b3d"));
    }
    
    @Test
    public void testSingleLiteralAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        
        Assert.assertEquals(1L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
    }
    
    @Test
    public void testMultipleLiteralsAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        Multimap<String,String> literals = LiteralNodeVisitor.getLiterals(script);
        
        Assert.assertEquals(2L, literals.size());
        Assert.assertTrue(literals.containsEntry("FOO", "1234"));
        Assert.assertTrue(literals.containsEntry("BAR", "abcd"));
    }
    
}
