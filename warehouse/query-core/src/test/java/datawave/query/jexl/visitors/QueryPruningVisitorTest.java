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
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    public void falseAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void falseAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertFalse(jexlState);
        
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertTrue(logAppender.getMessages().size() == 2);
        Assert.assertEquals("Pruning FIELD1 == 'x' && _NOFIELD_ == 'y' to false", logAppender.getMessages().get(0));
        Assert.assertEquals("Query before prune: FIELD1 == 'x' && _NOFIELD_ == 'y'\nQuery after prune: false", logAppender.getMessages().get(1));
    }
    
    @Test
    public void trueRewriteTest() throws ParseException {
        String query = "true || (FIELD1 == '1' && filter:isNull(FIELD2))";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertTrue(jexlState);
        
        Assert.assertEquals("true", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("true", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertTrue(logAppender.getMessages().size() == 2);
        Assert.assertEquals("Pruning true || (FIELD1 == '1' && filter:isNull(FIELD2)) to true", logAppender.getMessages().get(0));
        Assert.assertEquals("Query before prune: true || (FIELD1 == '1' && filter:isNull(FIELD2))\nQuery after prune: true", logAppender.getMessages().get(1));
    }
    
    @Test
    public void falseDoubleAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void falseDoubleAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertFalse(jexlState);
        
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertTrue(logAppender.getMessages().size() == 2);
        Assert.assertEquals("Pruning FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y' to false", logAppender.getMessages().get(0));
        Assert.assertEquals("Query before prune: FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'\nQuery after prune: false", logAppender.getMessages()
                        .get(1));
    }
    
    @Test
    public void unknownOrTest() throws ParseException {
        String query = "FIELD1 == 'x' || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void trueOrTest() throws ParseException {
        String query = "true || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void branchedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void branchedRewriteTest() throws ParseException {
        String subtree = "_NOFIELD_ == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals("FIELD1 == 'x'", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("FIELD1 == 'x'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertTrue(logAppender.getMessages().size() == 3);
        Assert.assertEquals("Pruning _NOFIELD_ == 'y' && FIELD2 == 'z' to false", logAppender.getMessages().get(0));
        Assert.assertEquals("Pruning (_NOFIELD_ == 'y' && FIELD2 == 'z') from FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')", logAppender.getMessages()
                        .get(1));
        Assert.assertEquals("Query before prune: FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')\nQuery after prune: FIELD1 == 'x'", logAppender
                        .getMessages().get(2));
    }
    
    @Test
    public void nestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertEquals(3, logAppender.getMessages().size());
        Assert.assertEquals("Pruning (_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z' to false", logAppender.getMessages().get(0));
        Assert.assertEquals(
                        "Pruning ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') from ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'",
                        logAppender.getMessages().get(1));
        Assert.assertEquals(
                        "Query before prune: ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'\nQuery after prune: FIELD2 == 'z'",
                        logAppender.getMessages().get(2));
    }
    
    @Test
    public void deeplyNestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        Assert.assertEquals(logAppender.getMessages().size() + "", 3, logAppender.getMessages().size());
        Assert.assertEquals("Pruning (_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z' to false",
                        logAppender.getMessages().get(0));
        Assert.assertEquals(
                        "Pruning ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') from ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'",
                        logAppender.getMessages().get(1));
        Assert.assertEquals(
                        "Query before prune: ((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'\nQuery after prune: FIELD2 == 'z'",
                        logAppender.getMessages().get(2));
    }
    
    @Test
    public void unchangedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (FIELD1 == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void unchangedRewriteTest() throws ParseException {
        String subtree = "FIELD1 == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void combinationTest() throws ParseException {
        String query = "F(_NOFIELD_) == 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void andTrueTest() throws ParseException {
        String query = "true && !(_NOFIELD_ == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void orFalseTest() throws ParseException {
        String query = "false || _NOFIELD_ == 'z'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notUnknownTest() throws ParseException {
        String query = "FIELD != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notFalseTest() throws ParseException {
        String query = "!false";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void notTrueTest() throws ParseException {
        String query = "!true";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void methodTest() throws ParseException {
        String query = "x('a')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void functionTest() throws ParseException {
        String query = "filter:isNull(FIELD)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GETest() throws ParseException {
        String query = "FIELD >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GTTest() throws ParseException {
        String query = "FIELD > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LETest() throws ParseException {
        String query = "FIELD <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LTTest() throws ParseException {
        String query = "FIELD < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.getState(script), QueryPruningVisitor.TruthState.UNKNOWN);
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void RETest() throws ParseException {
        String query = "FIELD =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void NRTest() throws ParseException {
        String query = "FIELD !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void GTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void LTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void RENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void NRNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
    }
    
    @Test
    public void markerTest() throws ParseException {
        String query = "(Assignment = true) && false";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void markerBoundedRangeTest() throws ParseException {
        String query = "((Assignment = true) && (FIELD > 'x' && FIELD < 'z'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void nestedMarkerBoundedRangeTest() throws ParseException {
        String query = "FIELD == 'b' || ((Assignment = true) && (FIELD > 'x' && FIELD < 'z'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void dualStatementQueryTest() throws ParseException {
        String query = "(Expression = 'somevalue'); FIELD == 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        query = "(Expression = 'somevalue'); FIELD == 'x' || true";
        script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
        Assert.assertEquals("(Expression = 'somevalue'); true", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
        
        query = "(Expression = 'somevalue'); FIELD == 'x' && false";
        script = JexlASTHelper.parseJexlQuery(query);
        
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        Assert.assertEquals("(Expression = 'somevalue'); false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    public void propertyMarkerTest() throws ParseException {
        String query = "((_Value_ = true) && (FIELD = 'x'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
        JexlNode newScript = QueryPruningVisitor.reduce(script, false);
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        Assert.assertEquals(query, newQuery);
        
        Assert.assertEquals(0, logAppender.getMessages().size(), logAppender.getMessages().size());
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
