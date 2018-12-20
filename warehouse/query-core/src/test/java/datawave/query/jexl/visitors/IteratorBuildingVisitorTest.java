package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IteratorBuildingVisitorTest {
    @Test
    public void buildLiteralRange_trailingWildcardTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO =~ 'bar.*'");
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);
        LiteralRange<?> range = IteratorBuildingVisitor.buildLiteralRange(erNodes.get(0));
        
        Assert.assertTrue(range.getLower().equals("bar"));
        Assert.assertTrue(range.isLowerInclusive());
        Assert.assertTrue(range.getUpper().equals("bar" + Constants.MAX_UNICODE_STRING));
        Assert.assertTrue(range.isUpperInclusive());
    }
    
    /**
     * For the sake of index lookups in the IteratorBuildingVisitor, all leading wildcards are full table FI scans since there is no reverse FI index
     * 
     * @throws ParseException
     */
    @Test
    public void buildLiteralRange_leadingWildcardTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO =~ '.*bar'");
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);
        LiteralRange<?> range = IteratorBuildingVisitor.buildLiteralRange(erNodes.get(0));
        
        Assert.assertTrue(range.getLower().equals(Constants.NULL_BYTE_STRING));
        Assert.assertTrue(range.isLowerInclusive());
        Assert.assertTrue(range.getUpper().equals(Constants.MAX_UNICODE_STRING));
        Assert.assertTrue(range.isUpperInclusive());
    }
    
    @Test
    public void buildLiteralRange_middleWildcardTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO =~ 'bar.*man'");
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);
        LiteralRange<?> range = IteratorBuildingVisitor.buildLiteralRange(erNodes.get(0));
        
        Assert.assertTrue(range.getLower().equals("bar"));
        Assert.assertTrue(range.isLowerInclusive());
        Assert.assertTrue(range.getUpper().equals("bar" + Constants.MAX_UNICODE_STRING));
        Assert.assertTrue(range.isUpperInclusive());
    }
    
    @Test
    public void buildLiteralRange_phraseTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO =~ 'barbaz'");
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(query);
        LiteralRange<?> range = IteratorBuildingVisitor.buildLiteralRange(erNodes.get(0));
        
        Assert.assertTrue(range.getLower().equals("barbaz"));
        Assert.assertTrue(range.isLowerInclusive());
        Assert.assertTrue(range.getUpper().equals("barbaz"));
        Assert.assertTrue(range.isUpperInclusive());
    }
}
