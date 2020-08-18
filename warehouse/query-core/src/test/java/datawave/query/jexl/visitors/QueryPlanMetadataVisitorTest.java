package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.QueryPlanMetadata;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryPlanMetadataVisitorTest {
    
    @Test
    public void testEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getEqNodeCount());
    }
    
    @Test
    public void testNotEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO != 'bar'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getNeNodeCount());
        assertTrue(planMetadata.hasNeNodes());
    }
    
    @Test
    public void testConjunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar' || FOO2 == 'baz'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(2, planMetadata.getEqNodeCount());
        assertEquals(0, planMetadata.getAndNodeCount());
        assertEquals(1, planMetadata.getOrNodeCount());
    }
    
    @Test
    public void testIntersection() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO == 'bar' && FOO2 == 'baz'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(2, planMetadata.getEqNodeCount());
        assertEquals(1, planMetadata.getAndNodeCount());
        assertEquals(0, planMetadata.getOrNodeCount());
    }
    
    @Test
    public void testRegexEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO =~ 'ba.*'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getErNodeCount());
    }
    
    @Test
    public void testRegexNotEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("FOO !~ 'ba.*'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getNrNodeCount());
    }
    
    @Test
    public void testLessThan() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("NUM < '1'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getLtNodeCount());
    }
    
    @Test
    public void testGreaterThan() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("NUM > '1'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getGtNodeCount());
    }
    
    @Test
    public void testLessThanEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("NUM <= '1'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getLeNodeCount());
    }
    
    @Test
    public void testGreaterThanEquals() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("NUM >= '1'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getGeNodeCount());
    }
    
    @Test
    public void testIncludeRegexFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:includeRegex(FOO, 'ba.*')");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getFNodeCount());
        assertEquals(1, planMetadata.getIncludeRegexCount());
    }
    
    @Test
    public void testExcludeRegexFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:excludeRegex(FOO, 'ba.*')");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getFNodeCount());
        assertEquals(1, planMetadata.getExcludeRegexCount());
    }
    
    @Test
    public void tesIsNullFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:isNull(FOO)");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getFNodeCount());
        assertEquals(1, planMetadata.getIsNullCount());
    }
    
    @Test
    public void testExceededOrValueThresholdMarkerJexlNode() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("((ExceededValueThresholdMarkerJexlNode = true) && (FOO == 'bar'))");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getAndNodeCount());
        assertEquals(1, planMetadata.getEqNodeCount());
        assertEquals(1, planMetadata.getExceededValueThresholdCount());
    }
    
    @Test
    public void testBetweenDatesFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getBetweenDatesCount());
        assertEquals(0, planMetadata.getBetweenLoadDatesCount());
    }
    
    @Test
    public void testBetweenLoadDatesFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:betweenLoadDates(LOAD_DATE, '20140101', '20140102', 'yyyyMMdd')");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(0, planMetadata.getBetweenDatesCount());
        assertEquals(1, planMetadata.getBetweenLoadDatesCount());
    }
    
    @Test
    public void testMatchesFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY')");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getMatchesAtLeastCount());
    }
    
    @Test
    public void testTimeFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getTimeFunctionCount());
    }
    
    @Test
    public void testIncludeTextFunction() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("filter.includeText(foo, bar)");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getIncludeTextCount());
    }
    
    @Test
    public void testAnyFieldPresent() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery("_ANYFIELD_ == 'bar'");
        QueryPlanMetadata planMetadata = QueryPlanMetadataVisitor.getQueryPlanMetadata(script);
        assertEquals(1, planMetadata.getEqNodeCount());
        assertTrue(planMetadata.hasAnyFieldNodes());
    }
}
