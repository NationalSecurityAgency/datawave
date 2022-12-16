package datawave.query.jexl.visitors;

import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple3;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryPruningVisitorTest {
    private static TestLogAppender logAppender;
    
    @BeforeClass
    public static void staticSetup() {
        logAppender = new TestLogAppender();
        Logger log = Logger.getLogger(QueryPruningVisitor.class);
        log.addAppender(logAppender);
        log.setLevel(Level.DEBUG);
    }
    
    @Before
    public void setup() {
        logAppender.clearMessages();
    }
    
    @Test
    public void delayedGeowaveTest() throws ParseException {
        String query = "geowave:intersects(GEO_FIELD, 'POINT(0 0)') && ((_Delayed_ = true) && (GEO_FIELD == '00' || GEO_FIELD == '0100' || false || false || GEO_FIELD == '0103'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        JexlNode rebuilt = QueryPruningVisitor.reduce(script, false);
        
        assertEquals("geowave:intersects(GEO_FIELD, 'POINT(0 0)') && ((_Delayed_ = true) && (GEO_FIELD == '00' || GEO_FIELD == '0100' || GEO_FIELD == '0103'))",
                        JexlStringBuildingVisitor.buildQuery(rebuilt));
    }
    
    // The MixedGeoAndGeoWaveTest fails the ASTValidator for reference expressions
    @Test
    public void testGeoQueryProblem() throws ParseException {
        String query = "((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && (false || false || false || false || false || GEO == '019821..0000000000' || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false)) || (geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && (false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || POINT == '1f200398c60112ee03' || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false || false)))";
        String expected = "((geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && GEO == '019821..0000000000') || (geowave:intersects(POINT, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && POINT == '1f200398c60112ee03'))";
        test(expected, query);
    }
    
    @Test
    public void testNarrowedGeoQueryProblem() throws ParseException {
        String query = "geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && (false || GEO == '019821..0000000000' || false)";
        String expected = "geowave:intersects(GEO, 'POLYGON((0.5 2, 1.5 2, 1.5 10, 0.5 10, 0.5 2))') && GEO == '019821..0000000000'";
        test(expected, query);
    }
    
    // this is an edge case
    @Ignore
    @Test
    public void testTinyScope() throws ParseException {
        String query = "(false || GEO == '019821..0000000000' || false)";
        String expected = "GEO == '019821..0000000000'";
        test(expected, query);
    }
    
    private void test(String expected, String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, false);
        
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(reduced));
    }
    
    @Test
    public void falseAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void falseAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        assertFalse(jexlState);
        
        assertEquals("false", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(2, logAppender.getMessages().size());
        assertEquals("Pruning FIELD1 == 'x' && _NOFIELD_ == 'y' to false", logAppender.getMessages().get(0));
        assertEquals("Query before prune: FIELD1 == 'x' && _NOFIELD_ == 'y'\nQuery after prune: false", logAppender.getMessages().get(1));
    }
    
    @Test
    public void trueRewriteTest() throws ParseException {
        String query = "true || (FIELD1 == '1' && filter:isNull(FIELD2))";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        assertTrue(jexlState);
        
        assertEquals("true", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("true", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(2, logAppender.getMessages().size());
        assertEquals("Pruning true || (FIELD1 == '1' && filter:isNull(FIELD2)) to true", logAppender.getMessages().get(0));
        assertEquals("Query before prune: true || (FIELD1 == '1' && filter:isNull(FIELD2))\nQuery after prune: true", logAppender.getMessages().get(1));
    }
    
    @Test
    public void falseDoubleAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void falseDoubleAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        assertFalse(jexlState);
        
        assertEquals("false", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(2, logAppender.getMessages().size());
        assertEquals("Pruning FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y' to false", logAppender.getMessages().get(0));
        assertEquals("Query before prune: FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'\nQuery after prune: false", logAppender.getMessages().get(1));
    }
    
    @Test
    public void unknownOrTest() throws ParseException {
        String query = "FIELD1 == 'x' || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void trueOrTest() throws ParseException {
        String query = "true || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void branchedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void branchedRewriteTest() throws ParseException {
        String subtree = "_NOFIELD_ == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        assertEquals("FIELD1 == 'x'", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("FIELD1 == 'x'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(3, logAppender.getMessages().size());
        assertEquals("Pruning _NOFIELD_ == 'y' && FIELD2 == 'z' to false", logAppender.getMessages().get(0));
        assertEquals("Pruning (_NOFIELD_ == 'y' && FIELD2 == 'z') from FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')", logAppender.getMessages().get(1));
        assertEquals("Query before prune: FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')\nQuery after prune: FIELD1 == 'x'", logAppender.getMessages()
                        .get(2));
    }
    
    @Test
    public void nestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(3, logAppender.getMessages().size());
        assertEquals("Pruning (_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z' to false", logAppender.getMessages().get(0));
        assertEquals("Pruning ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') from ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'",
                        logAppender.getMessages().get(1));
        assertEquals("Query before prune: ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'\nQuery after prune: FIELD2 == 'z'",
                        logAppender.getMessages().get(2));
    }
    
    @Test
    public void deeplyNestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        assertEquals(logAppender.getMessages().size() + "", 3, logAppender.getMessages().size());
        assertEquals("Pruning (_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z' to false", logAppender
                        .getMessages().get(0));
        assertEquals("Pruning ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') from ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'",
                        logAppender.getMessages().get(1));
        assertEquals("Query before prune: ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'\nQuery after prune: FIELD2 == 'z'",
                        logAppender.getMessages().get(2));
    }
    
    @Test
    public void unchangedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (FIELD1 == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void unchangedRewriteTest() throws ParseException {
        String subtree = "FIELD1 == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(reduced));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void combinationTest() throws ParseException {
        String query = "F(_NOFIELD_) == 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void andTrueTest() throws ParseException {
        String query = "true && !(_NOFIELD_ == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void orFalseTest() throws ParseException {
        String query = "false || _NOFIELD_ == 'z'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notUnknownTest() throws ParseException {
        String query = "FIELD != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notFalseTest() throws ParseException {
        String query = "!false";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notTrueTest() throws ParseException {
        String query = "!true";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void methodTest() throws ParseException {
        String query = "x('a')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void functionTest() throws ParseException {
        String query = "filter:isNull(FIELD)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GETest() throws ParseException {
        String query = "FIELD >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GTTest() throws ParseException {
        String query = "FIELD > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LETest() throws ParseException {
        String query = "FIELD <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LTTest() throws ParseException {
        String query = "FIELD < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void RETest() throws ParseException {
        String query = "FIELD =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void NRTest() throws ParseException {
        String query = "FIELD !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void RENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void NRNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void markerTest() throws ParseException {
        String query = "(Assignment = true) && false";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void markerBoundedRangeTest() throws ParseException {
        String query = "((Assignment = true) && (FIELD > 'x' && FIELD < 'z'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void nestedMarkerBoundedRangeTest() throws ParseException {
        String query = "FIELD == 'b' || ((Assignment = true) && (FIELD > 'x' && FIELD < 'z'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void dualStatementQueryTest() throws ParseException {
        String query = "(Expression = 'somevalue'); FIELD == 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        query = "(Expression = 'somevalue'); FIELD == 'x' || true";
        script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        assertEquals("(Expression = 'somevalue'); true", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        query = "(Expression = 'somevalue'); FIELD == 'x' && false";
        script = JexlASTHelper.parseJexlQuery(query);
        
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        assertEquals("(Expression = 'somevalue'); false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void propertyMarkerTest() throws ParseException {
        String query = "((_Value_ = true) && (FIELD = 'x'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        JexlNode newScript = QueryPruningVisitor.reduce(script, false);
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        assertEquals(query, newQuery);
        
        assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    private static class TestLogAppender extends AppenderSkeleton {
        private List<String> messages = new ArrayList<>();
        
        @Override
        public void close() {
            
        }
        
        @Override
        public boolean requiresLayout() {
            return false;
        }
        
        @Override
        protected void append(LoggingEvent loggingEvent) {
            messages.add(loggingEvent.getMessage().toString());
        }
        
        @Override
        public void doAppend(LoggingEvent event) {
            messages.add(event.getMessage().toString());
        }
        
        public List<String> getMessages() {
            return messages;
        }
        
        public void clearMessages() {
            messages = new ArrayList<>();
        }
    }
}
